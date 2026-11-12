"""FastAPI：单台 Modbus TCP 电表试采接口 + 电表读数归档接口。"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import threading
from typing import Annotated

from fastapi import Body, Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, StringConstraints, model_validator
from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException

from .archive_csv import CsvError, parse_readings_csv
from .archive_store import MAX_BLOCK_ROWS, ArchiveStore, StoreError
from .planner import PlanError, Segment, plan_reads

DEFAULT_HOST = os.getenv("METER_HOST", "meter")
DEFAULT_PORT = int(os.getenv("METER_PORT", "502"))
DEFAULT_SLAVE = int(os.getenv("METER_SLAVE_ID", "1"))
DEFAULT_TIMEOUT = float(os.getenv("METER_TIMEOUT", "3"))
DEFAULT_ARCHIVE_DB = "./data/archive.db"


class PointIn(BaseModel):
    address: int = Field(..., ge=0, description="保持寄存器起始地址（从 0 算）")
    words: int = Field(..., ge=1, le=2, description="字数：1（单字）或 2（双字，高字在前）")


class ReadRequest(BaseModel):
    points: list[PointIn] = Field(..., min_length=1, description="待试采测点，至少 1 个")
    range_start: int = Field(..., ge=0, description="设备可读区间起始地址（含两端）")
    range_end: int = Field(..., ge=0, description="设备可读区间结束地址（含两端）")
    max_read: int = Field(..., ge=1, description="单次读取寄存器字数上限")
    host: str = Field(default=DEFAULT_HOST, description="Modbus TCP 设备地址")
    port: int = Field(default=DEFAULT_PORT, ge=1, le=65535, description="Modbus TCP 端口")
    slave_id: int = Field(default=DEFAULT_SLAVE, ge=1, le=247, description="从站 / 单元号")
    timeout: float = Field(default=DEFAULT_TIMEOUT, gt=0, le=30, description="连接与读取超时（秒）")

    @model_validator(mode="after")
    def _check_range(self) -> "ReadRequest":
        if self.range_end < self.range_start:
            raise ValueError("range_end 不能小于 range_start")
        return self


class PointOut(BaseModel):
    index: int = Field(..., description="测点在请求 points 中的下标（从 0 算）")
    address: int
    words: int
    value: int = Field(..., description="无符号计数；双字时按高字在前（big-endian）拼接")


class SegmentOut(BaseModel):
    start: int = Field(..., description="读段起始地址（含）")
    end: int = Field(..., description="读段结束地址（含）")
    count: int = Field(..., description="本次读取的寄存器字数")
    point_indexes: list[int] = Field(..., description="该读段覆盖的测点下标")
    registers: list[int] = Field(..., description="读回的原始 16 位寄存器值（按地址升序）")


class ReadResponse(BaseModel):
    request_count: int = Field(..., description="实际 Modbus 请求次数（已最少化）")
    total_registers: int = Field(..., description="读取寄存器总数（次数相同时已最少化）")
    segments: list[SegmentOut]
    points: list[PointOut] = Field(..., description="按请求下标返回的测点评值")


# ---------------------------------------------------------------- 归档模型


class ArchiveRequest(BaseModel):
    """建档 / 更正请求：提交表号、UTC 起止秒与该时段的完整 CSV。"""

    meter_id: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)
    ] = Field(..., description="表号")
    start: int = Field(..., ge=0, description="时段起始 UTC 秒（含）")
    end: int = Field(..., gt=0, description="时段结束 UTC 秒（不含）")
    csv: str = Field(
        ...,
        description=(
            "该时段完整 CSV：表头 + 每行 `采样秒,累计计数`（32 位无符号）。"
            "采样秒须严格递增且落在时段内；只有表头表示清空该时段"
        ),
    )

    @model_validator(mode="after")
    def _check_window(self) -> "ArchiveRequest":
        if self.end <= self.start:
            raise ValueError("end 必须大于 start（时段含起秒、不含止秒）")
        return self


# Swagger 请求体样例：建档/更正 与 只有表头的清空
ARCHIVE_BODY_EXAMPLES = {
    "建档或更正一个时段": {
        "summary": "提交该时段完整 CSV（采样秒严格递增、落在 [start, end) 内）",
        "value": {
            "meter_id": "MTR-001",
            "start": 1735689600,
            "end": 1735689606,
            "csv": "ts,count\n"
            "1735689600,100042\n"
            "1735689601,100043\n"
            "1735689602,100045\n"
            "1735689603,100046\n"
            "1735689604,100048\n"
            "1735689605,100049\n",
        },
    },
    "清空一个时段": {
        "summary": "只有表头 = 清空该时段的读数",
        "value": {
            "meter_id": "MTR-001",
            "start": 1735689600,
            "end": 1735693200,
            "csv": "ts,count\n",
        },
    },
}


class BlockMetaOut(BaseModel):
    id: int = Field(..., description="块 ID（重写会产生新 ID，未动块 ID 不变）")
    first_ts: int = Field(..., description="块内首行采样秒")
    last_ts: int = Field(..., description="块内末行采样秒")
    row_count: int = Field(..., description=f"块内行数（每块最多 {MAX_BLOCK_ROWS} 行）")
    payload_bytes: int = Field(..., description="压缩负载字节数（zlib，可独立解压）")


class ArchiveResponse(BaseModel):
    meter_id: str
    start: int
    end: int
    rows_received: int = Field(..., description="新 CSV 的数据行数（0 表示清空该时段）")
    kept_outside_rows: int = Field(..., description="相交旧块中保留的段外读数行数")
    removed_blocks: list[BlockMetaOut] = Field(..., description="被重写的旧块")
    written_blocks: list[BlockMetaOut] = Field(..., description="新写入的块")


class ReadingOut(BaseModel):
    ts: int = Field(..., description="采样秒（UTC）")
    count: int = Field(..., description="32 位无符号累计计数")


class ArchiveQueryResponse(BaseModel):
    meter_id: str
    start: int
    end: int
    row_count: int = Field(..., description="时段内读数行数")
    blocks_read: int = Field(..., description="本次实际解压的块数（仅首尾范围相交的块）")
    rows: list[ReadingOut] = Field(..., description="按采样秒升序的原始读数")


class BlockListResponse(BaseModel):
    meter_id: str
    block_count: int
    max_block_rows: int
    blocks: list[BlockMetaOut]


# ---------------------------------------------------------------- 应用

app = FastAPI(
    title="Modbus TCP 电表试采与读数归档接口",
    version="1.1.0",
    description=(
        "试采：提交测点保持寄存器起始地址（从 0 算）与字数（1 或 2），"
        "并给出设备可读区间（含两端）与单次读取上限；接口先做读段规划"
        "（请求次数最少，次数相同则读取寄存器总数最少），再逐段读取，"
        "返回读段与无符号计数（双字高字在前）。无可行方案或读取失败时"
        "直接报错，不返回部分读数。\n\n"
        "归档：按表号提交某时段 [start, end) 的完整 CSV 建档或更正。"
        "读数按块（每块最多 512 行、zlib 压缩、可独立解压、记录首尾秒）"
        "持久化到 SQLite；更正只重写首尾范围与时段相交的旧块并保留段外读数，"
        "单事务提交，坏行或失败时原档不变，并发更正等同于逐次执行。"
    ),
)


def decode_unsigned(words: list[int]) -> int:
    """无符号拼接；双字高字在前（big-endian / 高字在低地址）。"""
    value = 0
    for word in words:
        value = (value << 16) | (word & 0xFFFF)
    return value


@app.get("/", include_in_schema=False)
def root() -> dict[str, str]:
    return {"service": "modbus-meter-sampling-archive", "docs": "/docs"}


@app.post(
    "/read",
    response_model=ReadResponse,
    tags=["试采"],
    summary="规划读段并对单台 Modbus TCP 电表执行试采",
    responses={
        422: {"description": "请求参数校验失败 / 无可行方案"},
        502: {"description": "连接或 Modbus 读取失败（不会返回部分读数）"},
    },
)
async def read_meter(req: ReadRequest) -> ReadResponse:
    try:
        segments: list[Segment] = plan_reads(
            points=[(p.address, p.words) for p in req.points],
            range_start=req.range_start,
            range_end=req.range_end,
            max_read=req.max_read,
        )
    except PlanError as exc:
        # 规划阶段失败：尚未与设备通信
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    client = AsyncModbusTcpClient(
        host=req.host,
        port=req.port,
        timeout=req.timeout,
    )
    try:
        await client.connect()
        if not client.connected:
            raise HTTPException(
                status_code=502,
                detail=f"连接 Modbus 设备失败：{req.host}:{req.port}",
            )

        seg_out: list[SegmentOut] = []
        # point_index -> 该测点读回的原始寄存器值
        point_words: dict[int, list[int]] = {}

        for seg in segments:
            try:
                rr = await client.read_holding_registers(
                    address=seg.start,
                    count=seg.count,
                    device_id=req.slave_id,
                )
            except (ModbusException, OSError, TimeoutError) as exc:
                raise HTTPException(
                    status_code=502,
                    detail=f"读取地址 {seg.start}..{seg.end} 失败：{exc}",
                ) from exc
            if rr.isError():
                raise HTTPException(
                    status_code=502,
                    detail=f"读取地址 {seg.start}..{seg.end} 失败：{rr}",
                )

            registers = list(rr.registers)
            seg_out.append(
                SegmentOut(
                    start=seg.start,
                    end=seg.end,
                    count=seg.count,
                    point_indexes=list(seg.point_indexes),
                    registers=registers,
                )
            )
            for idx in seg.point_indexes:
                p = req.points[idx]
                offset = p.address - seg.start
                point_words[idx] = registers[offset:offset + p.words]
    finally:
        client.close()

    points_out = [
        PointOut(
            index=i,
            address=p.address,
            words=p.words,
            value=decode_unsigned(point_words[i]),
        )
        for i, p in enumerate(req.points)
    ]
    return ReadResponse(
        request_count=len(segments),
        total_registers=sum(s.count for s in segments),
        segments=seg_out,
        points=points_out,
    )


# ---------------------------------------------------------------- 归档端点

_stores: dict[str, ArchiveStore] = {}
_stores_lock = threading.Lock()


def get_store() -> ArchiveStore:
    """按 ``ARCHIVE_DB_PATH`` 取得归档库（每路径一个实例，进程内缓存）。"""
    path = os.getenv("ARCHIVE_DB_PATH", DEFAULT_ARCHIVE_DB)
    with _stores_lock:
        store = _stores.get(path)
        if store is None:
            store = ArchiveStore(path)
            _stores[path] = store
        return store


@app.post(
    "/archive",
    response_model=ArchiveResponse,
    tags=["归档"],
    summary="归档或更正一个时段的电表读数",
    responses={
        422: {"description": "参数或 CSV 校验失败（坏行等），原档不变"},
        500: {"description": "归档读写失败，事务已回滚，原档不变"},
    },
)
async def archive_segment(
    req: Annotated[ArchiveRequest, Body(examples=ARCHIVE_BODY_EXAMPLES)],
    store: ArchiveStore = Depends(get_store),
) -> ArchiveResponse:
    # 先完整校验 CSV：任何坏行都在触碰归档之前失败
    try:
        rows = parse_readings_csv(req.csv, req.start, req.end)
    except CsvError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    try:
        result = await asyncio.to_thread(
            store.replace_segment, req.meter_id, req.start, req.end, rows
        )
    except (StoreError, sqlite3.Error, OSError) as exc:
        raise HTTPException(
            status_code=500, detail=f"归档写入失败，原档未变：{exc}"
        ) from exc

    return ArchiveResponse(
        meter_id=req.meter_id,
        start=req.start,
        end=req.end,
        rows_received=len(rows),
        kept_outside_rows=result.kept_outside_rows,
        removed_blocks=[b.as_dict() for b in result.removed_blocks],
        written_blocks=[b.as_dict() for b in result.written_blocks],
    )


@app.get(
    "/archive",
    response_model=ArchiveQueryResponse,
    tags=["归档"],
    summary="按表号与时段查询原始读数（只解压相交的块）",
    responses={
        422: {"description": "查询参数非法"},
        500: {"description": "归档读取失败"},
    },
)
async def query_archive(
    meter_id: str = Query(..., min_length=1, description="表号", examples=["MTR-001"]),
    start: int = Query(..., ge=0, description="时段起始 UTC 秒（含）", examples=[1735689600]),
    end: int = Query(..., gt=0, description="时段结束 UTC 秒（不含）", examples=[1735689610]),
    store: ArchiveStore = Depends(get_store),
) -> ArchiveQueryResponse:
    if end <= start:
        raise HTTPException(
            status_code=422, detail="end 必须大于 start（时段含起秒、不含止秒）"
        )
    try:
        rows, blocks_read = await asyncio.to_thread(
            store.query, meter_id, start, end
        )
    except (StoreError, sqlite3.Error, OSError) as exc:
        raise HTTPException(status_code=500, detail=f"归档读取失败：{exc}") from exc
    return ArchiveQueryResponse(
        meter_id=meter_id,
        start=start,
        end=end,
        row_count=len(rows),
        blocks_read=blocks_read,
        rows=[ReadingOut(ts=ts, count=cnt) for ts, cnt in rows],
    )


@app.get(
    "/archive/blocks",
    response_model=BlockListResponse,
    tags=["归档"],
    summary="查看该表的归档块元数据（演示 / 排障）",
    responses={500: {"description": "归档读取失败"}},
)
async def list_archive_blocks(
    meter_id: str = Query(..., min_length=1, description="表号", examples=["MTR-001"]),
    store: ArchiveStore = Depends(get_store),
) -> BlockListResponse:
    try:
        blocks = await asyncio.to_thread(store.list_blocks, meter_id)
    except (StoreError, sqlite3.Error, OSError) as exc:
        raise HTTPException(status_code=500, detail=f"归档读取失败：{exc}") from exc
    return BlockListResponse(
        meter_id=meter_id,
        block_count=len(blocks),
        max_block_rows=MAX_BLOCK_ROWS,
        blocks=[b.as_dict() for b in blocks],
    )
