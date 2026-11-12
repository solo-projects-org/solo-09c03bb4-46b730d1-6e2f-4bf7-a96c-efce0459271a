"""建档 / 更正 CSV 的解析与校验。

CSV 格式：
- 首行为表头，两个列名（约定 ``ts,count``；列名不强制，但首行两个字段
  都是数字时视为缺少表头，直接报错，防止把数据行当表头吞掉）；
- 其后每行两个字段：``采样秒,累计计数``；
  - 采样秒：UTC 秒，非负整数，须严格递增且落在时段 ``[start, end)`` 内；
  - 累计计数：32 位无符号整数（0..4294967295）；
- 空行（如文件末尾换行）忽略；只有表头没有数据行是合法输入，表示清空该时段。

任何坏行都抛出 :class:`CsvError`；调用方在解析通过前不触碰归档，
因此坏行时原档不变。
"""

from __future__ import annotations

import csv
import io
import re

MAX_UINT32 = (1 << 32) - 1
_UINT_RE = re.compile(r"\s*[0-9]+\s*")


class CsvError(ValueError):
    """CSV 内容非法（坏行、越界、乱序等）。"""


def _is_uint(text: str) -> bool:
    return _UINT_RE.fullmatch(text) is not None


def parse_readings_csv(text: str, start: int, end: int) -> list[tuple[int, int]]:
    """解析并校验 CSV，返回 ``(采样秒, 累计计数)`` 列表（按采样秒严格升序）。

    :param start: 时段起始 UTC 秒（含）
    :param end: 时段结束 UTC 秒（不含）
    :raises CsvError: 存在坏行时抛出，消息含行号与中文原因
    """
    if not text or not text.strip():
        raise CsvError("CSV 为空：至少需要表头行（只有表头表示清空该时段）")

    reader = csv.reader(io.StringIO(text))
    rows: list[tuple[int, int]] = []
    header_seen = False
    prev_ts = -1

    for record in reader:
        line_no = reader.line_num
        if not record:
            continue  # 空行（含文件末尾换行）
        cells = [cell.strip() for cell in record]

        if not header_seen:
            header_seen = True
            if len(cells) != 2 or not cells[0] or not cells[1]:
                raise CsvError(f"第 {line_no} 行：表头须为两列列名（如 ts,count）")
            if _is_uint(cells[0]) and _is_uint(cells[1]):
                raise CsvError(
                    f"第 {line_no} 行：首行疑似数据行，缺少表头（如 ts,count）"
                )
            continue

        if len(cells) != 2:
            raise CsvError(
                f"第 {line_no} 行：须为两列（采样秒,累计计数），实际 {len(record)} 列"
            )
        ts_raw, cnt_raw = cells
        if not _is_uint(ts_raw):
            raise CsvError(f"第 {line_no} 行：采样秒须为非负整数，实际为 {ts_raw!r}")
        ts = int(ts_raw)
        if not _is_uint(cnt_raw):
            raise CsvError(f"第 {line_no} 行：累计计数须为非负整数，实际为 {cnt_raw!r}")
        cnt = int(cnt_raw)
        if cnt > MAX_UINT32:
            raise CsvError(
                f"第 {line_no} 行：累计计数 {cnt} 超出 32 位无符号范围（0..{MAX_UINT32}）"
            )
        if ts <= prev_ts:
            raise CsvError(
                f"第 {line_no} 行：采样秒 {ts} 未严格递增（上一行为 {prev_ts}）"
            )
        if ts < start or ts >= end:
            raise CsvError(
                f"第 {line_no} 行：采样秒 {ts} 落在时段 [{start}, {end}) 之外"
            )
        prev_ts = ts
        rows.append((ts, cnt))

    return rows
