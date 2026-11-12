# Modbus TCP 电表试采与读数归档接口

两项能力：

1. **试采**：新电表接入能耗库前的单台 Modbus TCP 电表试采。厂务工程师提交测点的
   保持寄存器起始地址（从 0 算）与字数（1 或 2），并给出设备的可读区间
   （含两端）与单次读取上限；接口自动规划读段、按方案读表，返回读段与
   无符号计数（双字按高字在前拼接）。
2. **归档**：网关导出的电表读数留档，供能耗库复核。按表号提交某时段的完整
   CSV 建档；收到该时段的更正 CSV 时只重写受影响的归档块，不动整天的档案。
   读数以可独立解压的块（每块最多 512 行）持久化到 SQLite。

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

- API：<http://localhost:8000>，Swagger UI：<http://localhost:8000/docs>
- 模拟电表：`localhost:1502`（容器内 `meter:502`），保持寄存器 0..99
- 归档库：命名卷 `archive-data`（容器内 `/data/archive.db`），重启不丢

在 Swagger 中打开 `POST /read` → Try it out → 直接 Execute 即可对模拟表试采
（请求体默认指向容器内的模拟表）。`POST /archive` 自带「建档或更正」与
「清空时段」两个请求体样例，可直接 Execute 演示。

归档全流程演示脚本（建档 1500 行 → 分块 → 跨块更正 → 清空时段 → 复核查询）：

```bash
./demo/archive_demo.sh        # 或 BASE_URL=http://host:8000 ./demo/archive_demo.sh
```

## 归档接口

时段一律为半开区间 `[start, end)`：**含起秒、不含止秒**，单位 UTC 秒。

### `POST /archive` —— 归档或更正一个时段

请求体：

| 字段 | 说明 |
| --- | --- |
| `meter_id` | 表号 |
| `start` / `end` | 时段起止 UTC 秒（含起秒、不含止秒），`end > start` |
| `csv` | 该时段完整 CSV：表头 + 每行 `采样秒,累计计数` |

CSV 规则：

- 采样秒为非负整数，**严格递增**且全部落在 `[start, end)` 内；
- 累计计数为 **32 位无符号**整数（0..4294967295）；
- 只有表头没有数据行是合法输入，表示**清空该时段**；
- 任何坏行（列数不对、非数字、计数越界、乱序、采样秒出界）→ `422`，
  `detail` 给出行号与中文原因，**原档不变**。

更正语义：

- 只重写**首尾范围与时段相交**的旧块；旧块中落在时段外的读数保留，
  与新读数合并后按 512 行重新分块写回（不解压重写整天的档案）；
- 时段内以新文件为准；只有表头则清空该时段；
- 整个更正在一个事务内提交，读写失败自动回滚，**原档不变**；
- 并发更正由 SQLite 写锁串行化，结果等同于逐次执行；查询只会读到
  事务之前或之后的完整档案，不会读到更正了一半的状态。

响应（200）：`rows_received`（新 CSV 数据行数）、`kept_outside_rows`
（相交旧块中保留的段外行数）、`removed_blocks` / `written_blocks`
（被重写的旧块与新写入的块元数据）。

### `GET /archive` —— 查询原始读数

参数：`meter_id`、`start`、`end`（同上的时段规则）。按采样秒升序返回
`[start, end)` 内的原始读数；**只解压首尾范围与时段相交的块**，响应中的
`blocks_read` 是本次实际解压的块数。

### `GET /archive/blocks` —— 查看块元数据

参数：`meter_id`。返回该表全部块的 `id` / `first_ts` / `last_ts` /
`row_count` / `payload_bytes`，用于演示与排障（更正后未被重写的块 `id`
不变，可据此核对「只重写相交块」）。

### 归档存储设计

- SQLite（WAL 模式）单表 `blocks`：`meter_id`、`first_ts`、`last_ts`、
  `row_count`、`encoding`、`payload`；
- 每块为 `ts,count` 文本行的 zlib 压缩包，**最多 512 行、可独立解压**，
  块间无依赖；
- 更正与查询都按 `first_ts < end AND last_ts >= start` 过滤相交块；
- 更正事务：`BEGIN IMMEDIATE` → 读出相交旧块 → 保留段外读数 → 合并新读数
  → 删旧块 → 按 512 行重打包写回 → `COMMIT`；任一步失败整体 `ROLLBACK`。

### 演示：从建档到跨块更正

`demo/archive_demo.sh` 的完整流程（`T0 = 1735689600`，即 2025-01-01 00:00:00 UTC）：

```bash
# 1. 建档 [T0, T0+1500)，1500 行 -> 3 个块（512/512/476）
# 2. GET /archive/blocks 查看块首尾秒
# 3. 查询 [T0+500, T0+505) -> blocks_read=1（只解压相交块）
# 4. 跨块更正 [T0+400, T0+700)（横跨第 1、2 块）：
#    removed_blocks 为旧块 1、2，kept_outside_rows=724（段外保留），
#    724+300=1024 行重打包为 2 个新块；第 3 块 id 不变、未被触碰
# 5. 清空 [T0+100, T0+200)：提交只有表头的 CSV
# 6. 复核：399 秒仍是旧值、400 秒起是新值；全天 1500-100=1400 行
```

手动 curl 示例（更正 [T0+400, T0+700)）：

```bash
curl -X POST http://localhost:8000/archive \
  -H 'Content-Type: application/json' \
  -d '{"meter_id":"MTR-001","start":1735690000,"end":1735690300,
       "csv":"ts,count\n1735690000,555000\n1735690001,555001\n"}'

curl 'http://localhost:8000/archive?meter_id=MTR-001&start=1735689998&end=1735690002'
```

## 试采接口 `POST /read`

请求体：

| 字段 | 说明 |
| --- | --- |
| `points` | 测点列表，`address` 为保持寄存器起始地址（从 0 算），`words` 为字数（1 或 2） |
| `range_start` / `range_end` | 设备可读区间，含两端 |
| `max_read` | 单次读取寄存器字数上限 |
| `host` / `port` / `slave_id` / `timeout` | 设备连接参数，默认指向 Compose 中的模拟表 |

读段规划规则：

1. 每次读取一段连续寄存器，读段整体落在可读区间内，字数不超过 `max_read`；
2. 每个测点必须被某一次读取完整覆盖（不允许跨段拼接）；
3. 先使请求次数最少；次数相同时使读取寄存器总数最少
   （按地址排序后动态规划求解，读段紧贴测点，不多读无用寄存器）。

响应（200）：

- `request_count` / `total_registers`：实际请求次数与读取寄存器总数；
- `segments`：各读段的起止地址（含）、字数、覆盖的测点下标、原始寄存器值；
- `points`：按请求下标返回的测点值，`value` 为无符号计数，
  双字按高字在前（big-endian）拼接。

错误：

- `422`：参数校验失败或无可行方案（测点超出可读区间、字数超过单次上限等），
  `detail` 为中文原因；
- `502`：连接失败或 Modbus 读取失败（含设备异常响应），**不返回部分读数**。

```bash
curl -X POST http://localhost:8000/read \
  -H 'Content-Type: application/json' \
  -d '{
    "points": [
      {"address": 0, "words": 2},
      {"address": 2, "words": 2},
      {"address": 10, "words": 1},
      {"address": 12, "words": 1}
    ],
    "range_start": 0,
    "range_end": 99,
    "max_read": 8
  }'
```

## 模拟电表数据

`simulator/meter_sim.py`：保持寄存器 0..99，可读区间 `[0, 99]`。

| 地址 | 值 | 说明 |
| --- | --- | --- |
| 0..1 | 0x0001, 0x86A0 | 32 位无符号 100000（高字在前） |
| 2..3 | 0x0000, 0x0064 | 32 位无符号 100 |
| 10..13 | 1234, 5678, 65535, 42 | 单字演示值 |
| 其余 | `(addr * 37 + 11) & 0xFFFF` | 确定性填充，便于人工核对 |

## 本地开发（无 Docker）

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
METER_PORT=1502 .venv/bin/python -m simulator.meter_sim &
METER_HOST=127.0.0.1 METER_PORT=1502 .venv/bin/uvicorn app.main:app --port 8000
.venv/bin/pytest            # 40 个用例：规划 + 试采集成 + 归档（含并发/原子性）
```

环境变量：

- `METER_HOST` / `METER_PORT` / `METER_SLAVE_ID` / `METER_TIMEOUT`：
  `/read` 请求体连接参数的默认值（Compose 中已注入）；
- `ARCHIVE_DB_PATH`：归档库文件路径，默认 `./data/archive.db`
  （Compose 中为 `/data/archive.db`，挂命名卷持久化）。

## 目录结构

```
app/main.py          FastAPI 接口：/read 试采 + /archive 归档三端点
app/planner.py       读段规划（动态规划：次数最少 -> 寄存器总数最少）
app/archive_csv.py   建档/更正 CSV 解析与校验（坏行报行号）
app/archive_store.py SQLite 块存储：512 行 zlib 块、事务性时段替换
simulator/           pymodbus 内存模拟电表
tests/               规划单元测试 + 试采集成测试 + 归档测试（无需 Docker）
demo/archive_demo.sh 建档 -> 跨块更正 -> 清空 -> 复核查询 全流程演示
docker-compose.yml   api + meter 两个服务，归档库挂命名卷
```
