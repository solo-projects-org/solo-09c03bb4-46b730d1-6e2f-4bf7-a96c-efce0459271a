"""电表读数归档存储：SQLite 持久化、可独立解压的读数块。

存储模型
--------
- 每条块记录保存一段压缩读数：块内为 ``ts,count`` 文本行的 zlib 压缩包，
  每块最多 :data:`MAX_BLOCK_ROWS` 行，可独立解压，块间无依赖；
- 块表记录每块的首尾采样秒（``first_ts`` / ``last_ts``），更正与查询都按
  “块区间与时段 [start, end) 相交”（``first_ts < end AND last_ts >= start``）
  过滤，只解压相交的块，不重写整天的档案。

更正语义（:meth:`ArchiveStore.replace_segment`）
------------------------------------------------
单个 ``BEGIN IMMEDIATE`` 事务内完成：读出相交旧块 → 保留其中落在时段外的
读数 → 与新读数合并排序 → 删除旧块 → 按 512 行重打包写回。任何一步失败
整体回滚，原档不变。SQLite 写锁使并发更正等同于逐次执行；WAL 模式下
查询只会读到事务之前或之后的完整档案，不会读到更正了一半的状态。
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
import zlib
from dataclasses import asdict, dataclass

MAX_BLOCK_ROWS = 512
ENCODING = "zlib:ts-count"
_BUSY_TIMEOUT_MS = 30_000

SCHEMA = """
CREATE TABLE IF NOT EXISTS blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    meter_id TEXT NOT NULL,
    first_ts INTEGER NOT NULL,
    last_ts INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    encoding TEXT NOT NULL,
    payload BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_blocks_meter_span
    ON blocks (meter_id, first_ts, last_ts);
"""

_INTERSECT = "meter_id = ? AND first_ts < ? AND last_ts >= ?"


class StoreError(Exception):
    """归档读写失败（写事务已回滚，原档不变）。"""


@dataclass(frozen=True)
class BlockMeta:
    """块元数据（不含压缩负载）。"""

    id: int
    meter_id: str
    first_ts: int
    last_ts: int
    row_count: int
    payload_bytes: int

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class ReplaceResult:
    """一次时段替换的结果。"""

    kept_outside_rows: int  # 相交旧块中保留的段外读数行数
    removed_blocks: list[BlockMeta]  # 被重写的旧块
    written_blocks: list[BlockMeta]  # 新写入的块


def encode_rows(rows: list[tuple[int, int]]) -> bytes:
    """把按采样秒升序的读数编码为可独立解压的块负载。"""
    text = "".join(f"{ts},{cnt}\n" for ts, cnt in rows)
    return zlib.compress(text.encode("ascii"))


def decode_rows(payload: bytes, block_id: int | None = None) -> list[tuple[int, int]]:
    """解压块负载；负载损坏时抛出 :class:`StoreError`。"""
    where = f"块 #{block_id} " if block_id is not None else ""
    try:
        text = zlib.decompress(payload).decode("ascii")
        rows: list[tuple[int, int]] = []
        for line in text.splitlines():
            parts = line.split(",")
            if len(parts) != 2:
                raise ValueError(f"行格式非法：{line!r}")
            rows.append((int(parts[0]), int(parts[1])))
        return rows
    except (zlib.error, UnicodeDecodeError, ValueError) as exc:
        raise StoreError(f"{where}解压失败：{exc}") from exc


class ArchiveStore:
    """基于 SQLite 文件的归档库。

    每个方法自带连接、各自独立，可被 asyncio 线程池并发调用；
    写操作由 SQLite 锁串行化。
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        conn = self._connect()
        try:
            conn.executescript(SCHEMA)
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            timeout=_BUSY_TIMEOUT_MS / 1000,
            isolation_level=None,  # 自动提交；事务由 SQL 显式控制
        )
        conn.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

    # -- 建档 / 更正 ----------------------------------------------------

    def replace_segment(
        self,
        meter_id: str,
        start: int,
        end: int,
        rows: list[tuple[int, int]],
    ) -> ReplaceResult:
        """用新读数替换时段 ``[start, end)``（单事务，失败整体回滚）。

        只重写首尾范围与时段相交的旧块，旧块中落在时段外的读数保留；
        ``rows`` 为空表示清空该时段。

        :param rows: 已校验的新读数 ``[(ts, count), ...]``，全部落在时段内
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            old = conn.execute(
                f"SELECT id, first_ts, last_ts, row_count, length(payload), payload"
                f" FROM blocks WHERE {_INTERSECT} ORDER BY first_ts, id",
                (meter_id, end, start),
            ).fetchall()

            # 保留相交旧块中落在时段外的读数
            kept: list[tuple[int, int]] = []
            for block_id, _f, _l, _rc, _sz, payload in old:
                for ts, cnt in decode_rows(payload, block_id):
                    if ts < start or ts >= end:
                        kept.append((ts, cnt))

            merged = sorted([*kept, *rows])

            conn.executemany(
                "DELETE FROM blocks WHERE id = ?", [(rec[0],) for rec in old]
            )
            written: list[BlockMeta] = []
            for i in range(0, len(merged), MAX_BLOCK_ROWS):
                chunk = merged[i : i + MAX_BLOCK_ROWS]
                payload = encode_rows(chunk)
                cur = conn.execute(
                    "INSERT INTO blocks"
                    " (meter_id, first_ts, last_ts, row_count, encoding, payload)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        meter_id,
                        chunk[0][0],
                        chunk[-1][0],
                        len(chunk),
                        ENCODING,
                        payload,
                    ),
                )
                written.append(
                    BlockMeta(
                        id=cur.lastrowid,
                        meter_id=meter_id,
                        first_ts=chunk[0][0],
                        last_ts=chunk[-1][0],
                        row_count=len(chunk),
                        payload_bytes=len(payload),
                    )
                )
            conn.execute("COMMIT")
            return ReplaceResult(
                kept_outside_rows=len(kept),
                removed_blocks=[
                    BlockMeta(
                        id=rec[0],
                        meter_id=meter_id,
                        first_ts=rec[1],
                        last_ts=rec[2],
                        row_count=rec[3],
                        payload_bytes=rec[4],
                    )
                    for rec in old
                ],
                written_blocks=written,
            )
        except BaseException:
            with contextlib.suppress(sqlite3.Error):
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    # -- 查询 ------------------------------------------------------------

    def query(
        self, meter_id: str, start: int, end: int
    ) -> tuple[list[tuple[int, int]], int]:
        """查询时段 ``[start, end)`` 内的原始读数。

        只解压首尾范围与时段相交的块。
        :return: ``(按采样秒升序的 [(ts, count), ...], 实际解压的块数)``
        """
        conn = self._connect()
        try:
            cur = conn.execute(
                f"SELECT id, payload FROM blocks WHERE {_INTERSECT}"
                f" ORDER BY first_ts, id",
                (meter_id, end, start),
            )
            rows: list[tuple[int, int]] = []
            blocks_read = 0
            for block_id, payload in cur:
                blocks_read += 1
                for ts, cnt in decode_rows(payload, block_id):
                    if start <= ts < end:
                        rows.append((ts, cnt))
            rows.sort()
            return rows, blocks_read
        finally:
            conn.close()

    def list_blocks(self, meter_id: str) -> list[BlockMeta]:
        """列出该表全部块的元数据（按首秒排序），用于演示与排障。"""
        conn = self._connect()
        try:
            cur = conn.execute(
                "SELECT id, first_ts, last_ts, row_count, length(payload)"
                " FROM blocks WHERE meter_id = ? ORDER BY first_ts, id",
                (meter_id,),
            )
            return [
                BlockMeta(
                    id=rec[0],
                    meter_id=meter_id,
                    first_ts=rec[1],
                    last_ts=rec[2],
                    row_count=rec[3],
                    payload_bytes=rec[4],
                )
                for rec in cur
            ]
        finally:
            conn.close()
