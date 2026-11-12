"""归档 API 与存储测试。

覆盖：建档 -> 查询回读、512 行分块、跨块更正（只重写相交块、段外保留）、
表头only清空、坏行 / 写失败时原档不变、并发更正等同于逐次执行、
查询只解压相交块。不依赖 Docker。
"""

from __future__ import annotations

import sqlite3
import threading

import httpx
import pytest

from app.archive_csv import CsvError, parse_readings_csv
from app.archive_store import MAX_BLOCK_ROWS, ArchiveStore, StoreError
from app.main import app, get_store

BASE_TS = 1_735_689_600  # 2025-01-01T00:00:00Z
METER = "MTR-001"


@pytest.fixture
def store(tmp_path):
    """每用例一个独立归档库，并通过依赖覆盖注入 FastAPI。"""
    store = ArchiveStore(str(tmp_path / "archive.db"))
    app.dependency_overrides[get_store] = lambda: store
    yield store
    app.dependency_overrides.clear()


async def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    )


def make_csv(first_ts: int, n: int, first_value: int = 100_000) -> str:
    lines = ["ts,count"]
    for i in range(n):
        lines.append(f"{first_ts + i},{first_value + i}")
    return "\n".join(lines) + "\n"


def archive_payload(start: int, end: int, csv: str, meter: str = METER) -> dict:
    return {"meter_id": meter, "start": start, "end": end, "csv": csv}


async def _post_archive(payload: dict) -> httpx.Response:
    async with await _client() as client:
        return await client.post("/archive", json=payload)


async def _get_archive(start: int, end: int, meter: str = METER) -> httpx.Response:
    async with await _client() as client:
        return await client.get(
            "/archive", params={"meter_id": meter, "start": start, "end": end}
        )


async def _get_blocks(meter: str = METER) -> httpx.Response:
    async with await _client() as client:
        return await client.get("/archive/blocks", params={"meter_id": meter})


# ---------------------------------------------------------------- API：建档与查询


async def test_archive_then_query_roundtrip(store: ArchiveStore) -> None:
    resp = await _post_archive(
        archive_payload(BASE_TS, BASE_TS + 10, make_csv(BASE_TS, 10, 7_000))
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["rows_received"] == 10
    assert body["kept_outside_rows"] == 0
    assert body["removed_blocks"] == []
    assert len(body["written_blocks"]) == 1
    assert body["written_blocks"][0]["row_count"] == 10

    resp = await _get_archive(BASE_TS, BASE_TS + 10)
    assert resp.status_code == 200
    data = resp.json()
    assert data["row_count"] == 10
    assert data["blocks_read"] == 1
    assert [(r["ts"], r["count"]) for r in data["rows"]] == [
        (BASE_TS + i, 7_000 + i) for i in range(10)
    ]


async def test_period_is_half_open(store: ArchiveStore) -> None:
    await _post_archive(archive_payload(BASE_TS, BASE_TS + 10, make_csv(BASE_TS, 10)))
    # 查询 [t, t+9)：不含第 9 秒；查询 [t+1, t+10)：不含第 0 秒
    resp = await _get_archive(BASE_TS, BASE_TS + 9)
    assert [r["ts"] for r in resp.json()["rows"]] == [BASE_TS + i for i in range(9)]
    resp = await _get_archive(BASE_TS + 1, BASE_TS + 10)
    assert [r["ts"] for r in resp.json()["rows"]] == [
        BASE_TS + i for i in range(1, 10)
    ]
    # 采样秒等于 end 的行属于下一段，本段提交会被拒绝
    resp = await _post_archive(
        archive_payload(BASE_TS, BASE_TS + 10, make_csv(BASE_TS, 11))
    )
    assert resp.status_code == 422
    assert "落在时段" in resp.json()["detail"]


async def test_archive_chunks_into_512_row_blocks(store: ArchiveStore) -> None:
    resp = await _post_archive(
        archive_payload(BASE_TS, BASE_TS + 1500, make_csv(BASE_TS, 1500))
    )
    assert resp.status_code == 200
    assert [b["row_count"] for b in resp.json()["written_blocks"]] == [
        MAX_BLOCK_ROWS,
        MAX_BLOCK_ROWS,
        476,
    ]

    resp = await _get_blocks()
    blocks = resp.json()["blocks"]
    assert resp.json()["block_count"] == 3
    assert [b["row_count"] for b in blocks] == [512, 512, 476]
    assert blocks[0]["first_ts"] == BASE_TS
    assert blocks[0]["last_ts"] == BASE_TS + 511
    assert blocks[2]["last_ts"] == BASE_TS + 1499


async def test_query_decompresses_only_intersecting_blocks(
    store: ArchiveStore,
) -> None:
    await _post_archive(
        archive_payload(BASE_TS, BASE_TS + 1500, make_csv(BASE_TS, 1500))
    )
    # 完全落在第 1 块内
    resp = await _get_archive(BASE_TS + 10, BASE_TS + 20)
    assert resp.json()["blocks_read"] == 1
    assert resp.json()["row_count"] == 10
    # 跨第 1、2 块边界
    resp = await _get_archive(BASE_TS + 500, BASE_TS + 600)
    assert resp.json()["blocks_read"] == 2
    assert resp.json()["row_count"] == 100
    # 空表号：不解压任何块
    resp = await _get_archive(BASE_TS, BASE_TS + 1500, meter="NOPE")
    assert resp.json()["blocks_read"] == 0
    assert resp.json()["row_count"] == 0


async def test_query_validation(store: ArchiveStore) -> None:
    resp = await _get_archive(BASE_TS + 10, BASE_TS + 10)
    assert resp.status_code == 422
    resp = await _get_archive(BASE_TS + 10, BASE_TS + 9)
    assert resp.status_code == 422
    async with await _client() as client:
        resp = await client.get("/archive", params={"start": 0, "end": 1})
    assert resp.status_code == 422  # 缺 meter_id


# ---------------------------------------------------------------- API：更正


async def test_cross_block_correction_rewrites_only_intersecting_blocks(
    store: ArchiveStore,
) -> None:
    await _post_archive(
        archive_payload(BASE_TS, BASE_TS + 1500, make_csv(BASE_TS, 1500))
    )
    before = (await _get_blocks()).json()["blocks"]
    assert [b["row_count"] for b in before] == [512, 512, 476]

    # 更正 [T0+400, T0+700)：横跨第 1、2 块，不碰第 3 块
    resp = await _post_archive(
        archive_payload(BASE_TS + 400, BASE_TS + 700, make_csv(BASE_TS + 400, 300, 555_000))
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["rows_received"] == 300
    # 段外保留：第 1 块的 0..399 共 400 行 + 第 2 块的 700..1023 共 324 行
    assert body["kept_outside_rows"] == 724
    assert {b["id"] for b in body["removed_blocks"]} == {
        before[0]["id"],
        before[1]["id"],
    }

    after = (await _get_blocks()).json()["blocks"]
    # 第 3 块未被重写：ID 与内容原样保留
    assert after[-1]["id"] == before[2]["id"]
    assert after[-1]["first_ts"] == before[2]["first_ts"]
    # 724 + 300 = 1024 行 -> 恰好 2 个新块
    assert [b["row_count"] for b in after] == [512, 512, 476]

    # 全量复核：段外是旧值，段内是新值
    resp = await _get_archive(BASE_TS, BASE_TS + 1500)
    rows = resp.json()["rows"]
    assert resp.json()["row_count"] == 1500
    for i in range(1500):
        ts, cnt = rows[i]["ts"], rows[i]["count"]
        assert ts == BASE_TS + i
        expected = 555_000 + (i - 400) if 400 <= i < 700 else 100_000 + i
        assert cnt == expected, f"ts={ts}"


async def test_header_only_csv_clears_segment(store: ArchiveStore) -> None:
    await _post_archive(
        archive_payload(BASE_TS, BASE_TS + 1000, make_csv(BASE_TS, 1000))
    )
    resp = await _post_archive(
        archive_payload(BASE_TS + 100, BASE_TS + 200, "ts,count\n")
    )
    assert resp.status_code == 200
    assert resp.json()["rows_received"] == 0

    resp = await _get_archive(BASE_TS, BASE_TS + 1000)
    data = resp.json()
    assert data["row_count"] == 900
    assert [r["ts"] for r in data["rows"]] == [
        BASE_TS + i for i in range(1000) if not 100 <= i < 200
    ]


async def test_correction_on_empty_archive_just_inserts(
    store: ArchiveStore,
) -> None:
    resp = await _post_archive(
        archive_payload(BASE_TS, BASE_TS + 5, make_csv(BASE_TS, 5))
    )
    assert resp.status_code == 200
    assert resp.json()["removed_blocks"] == []
    assert resp.json()["rows_received"] == 5


BAD_CSVS = [
    ("", "至少需要表头"),
    ("100,1\n200,2\n", "缺少表头"),  # 首行像数据
    ("ts,count,extra\n1,2,3\n", "表头须为两列"),
    ("ts,count\n100\n", "须为两列"),
    ("ts,count\nabc,1\n", "采样秒须为非负整数"),
    ("ts,count\n100,abc\n", "累计计数须为非负整数"),
    ("ts,count\n100,-1\n", "累计计数须为非负整数"),
    ("ts,count\n100,4294967296\n", "超出 32 位无符号"),
    # 采样秒须落在所提交时段内（此处为 [BASE_TS+100, BASE_TS+200)）才能走到递增校验
    (f"ts,count\n{BASE_TS + 150},1\n{BASE_TS + 149},2\n", "未严格递增"),
    (f"ts,count\n{BASE_TS + 150},1\n{BASE_TS + 150},2\n", "未严格递增"),
]


@pytest.mark.parametrize("bad_csv,reason", BAD_CSVS)
async def test_bad_csv_returns_422_and_archive_unchanged(
    store: ArchiveStore, bad_csv: str, reason: str
) -> None:
    await _post_archive(
        archive_payload(BASE_TS, BASE_TS + 1000, make_csv(BASE_TS, 1000))
    )
    before_rows = (await _get_archive(BASE_TS, BASE_TS + 1000)).json()["rows"]
    before_blocks = (await _get_blocks()).json()["blocks"]

    resp = await _post_archive(
        archive_payload(BASE_TS + 100, BASE_TS + 200, bad_csv)
    )
    assert resp.status_code == 422
    assert reason in resp.json()["detail"]

    # 原档不变：读数与块都保持原样
    assert (await _get_archive(BASE_TS, BASE_TS + 1000)).json()["rows"] == before_rows
    assert (await _get_blocks()).json()["blocks"] == before_blocks


async def test_ts_outside_segment_rejected(store: ArchiveStore) -> None:
    # 采样秒落在 [start, end) 之外
    resp = await _post_archive(
        archive_payload(BASE_TS + 100, BASE_TS + 200, make_csv(BASE_TS + 50, 10))
    )
    assert resp.status_code == 422
    assert "落在时段" in resp.json()["detail"]


async def test_meters_are_isolated(store: ArchiveStore) -> None:
    await _post_archive(archive_payload(BASE_TS, BASE_TS + 10, make_csv(BASE_TS, 10)))
    resp = await _get_archive(BASE_TS, BASE_TS + 10, meter="MTR-002")
    assert resp.json()["row_count"] == 0
    resp = await _get_blocks(meter="MTR-002")
    assert resp.json()["block_count"] == 0


# ---------------------------------------------------------------- 存储层：原子性与并发


def test_failed_replace_rolls_back(store: ArchiveStore) -> None:
    """读写失败（如块负载损坏）时事务回滚，原档不变。"""
    store.replace_segment(METER, BASE_TS, BASE_TS + 1000, [(BASE_TS + i, i) for i in range(1000)])
    blocks_before = store.list_blocks(METER)
    assert len(blocks_before) == 2

    # 直接破坏第 1 块的负载，使更正时的解压失败
    conn = sqlite3.connect(store.db_path)
    conn.execute(
        "UPDATE blocks SET payload = ? WHERE id = ?",
        (b"not-zlib", blocks_before[0].id),
    )
    conn.commit()
    conn.close()

    with pytest.raises(StoreError):
        store.replace_segment(
            METER, BASE_TS + 100, BASE_TS + 200, [(BASE_TS + 100, 1)]
        )

    # 原档不变：没有块被删除或新增（负载字节数因测试人为破坏而不在比较范围）
    after = store.list_blocks(METER)
    assert [b.id for b in after] == [b.id for b in blocks_before]
    assert [(b.first_ts, b.last_ts, b.row_count) for b in after] == [
        (b.first_ts, b.last_ts, b.row_count) for b in blocks_before
    ]


def test_concurrent_disjoint_corrections_equal_sequential(
    store: ArchiveStore,
) -> None:
    """8 个线程并发更正互不相交的时段，结果等同于逐次执行。"""
    n = 2000
    store.replace_segment(METER, BASE_TS, BASE_TS + n, [(BASE_TS + i, i) for i in range(n)])

    def worker(k: int) -> None:
        s = BASE_TS + k * 100
        store.replace_segment(METER, s, s + 100, [(s + i, 900_000 + k) for i in range(100)])

    threads = [threading.Thread(target=worker, args=(k,)) for k in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    rows, _ = store.query(METER, BASE_TS, BASE_TS + n)
    assert len(rows) == n
    for i, (ts, cnt) in enumerate(rows):
        assert ts == BASE_TS + i
        segment = i // 100
        expected = 900_000 + segment if segment < 8 else i
        assert cnt == expected, f"ts={ts}"


def test_concurrent_same_range_corrections_no_interleave(
    store: ArchiveStore,
) -> None:
    """并发更正同一时段：最终结果须完整等于某一次提交，不得交叉混杂。"""
    s = BASE_TS
    store.replace_segment(METER, s, s + 50, [(s + i, 0) for i in range(50)])

    def worker(k: int) -> None:
        store.replace_segment(METER, s, s + 50, [(s + i, 1_000 * k) for i in range(50)])

    threads = [threading.Thread(target=worker, args=(k,)) for k in range(1, 7)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    rows, _ = store.query(METER, s, s + 50)
    assert len(rows) == 50
    winners = {cnt for _, cnt in rows}
    assert len(winners) == 1  # 全部行来自同一次提交
    assert winners.pop() in {1_000 * k for k in range(1, 7)}


def test_query_never_reads_half_applied_correction(
    store: ArchiveStore,
) -> None:
    """更正进行中的并发查询：每行只能是旧值或新值，行数恒定。"""
    n = 800
    store.replace_segment(METER, BASE_TS, BASE_TS + n, [(BASE_TS + i, 1) for i in range(n)])

    def worker(k: int) -> None:
        s = BASE_TS + k * 200
        store.replace_segment(METER, s, s + 200, [(s + i, 2) for i in range(200)])

    threads = [threading.Thread(target=worker, args=(k,)) for k in range(4)]
    for t in threads:
        t.start()
    while any(t.is_alive() for t in threads):
        rows, _ = store.query(METER, BASE_TS, BASE_TS + n)
        assert len(rows) == n  # 不会出现“删了旧块还没写新块”的中间态
        assert all(cnt in (1, 2) for _, cnt in rows)
    for t in threads:
        t.join()

    rows, _ = store.query(METER, BASE_TS, BASE_TS + n)
    assert [cnt for _, cnt in rows] == [2] * n


# ---------------------------------------------------------------- CSV 解析单元测试


def test_parse_csv_header_only_means_clear() -> None:
    assert parse_readings_csv("ts,count\n", 0, 100) == []
    assert parse_readings_csv("ts,count", 0, 100) == []


def test_parse_csv_strips_whitespace_and_skips_blank_lines() -> None:
    rows = parse_readings_csv("ts,count\n\n 10 , 42 \n11,43\n\n", 0, 100)
    assert rows == [(10, 42), (11, 43)]


def test_parse_csv_uint32_boundary() -> None:
    rows = parse_readings_csv("ts,count\n10,4294967295\n", 0, 100)
    assert rows == [(10, 4_294_967_295)]
    with pytest.raises(CsvError):
        parse_readings_csv("ts,count\n10,4294967296\n", 0, 100)
