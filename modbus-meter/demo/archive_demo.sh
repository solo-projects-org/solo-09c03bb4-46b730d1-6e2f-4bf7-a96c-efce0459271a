#!/usr/bin/env bash
# 电表读数归档演示：建档 -> 查询 -> 跨块更正 -> 清空时段 -> 复核查询
#
# 前置：docker compose up --build 已启动（API 默认 http://localhost:8000）。
# 用法：./demo/archive_demo.sh   或   BASE_URL=http://host:8000 ./demo/archive_demo.sh
# 仅依赖 bash / awk / curl；若装有 jq 会自动美化 JSON 输出。
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
METER="${METER_ID:-MTR-001}"
T0=1735689600  # 2025-01-01 00:00:00 UTC

pretty() {  # 有 jq 美化，否则原样输出
  if command -v jq >/dev/null 2>&1; then jq .; else cat; fi
  echo
}

step() { printf '\n========== %s ==========\n' "$*"; }

# 生成 CSV：$1=首行采样秒 $2=行数 $3=首个累计计数（每秒一行，计数逐秒 +1）
make_csv() {
  awk -v s="$1" -v n="$2" -v v="$3" 'BEGIN{
    print "ts,count"
    for (i = 0; i < n; i++) printf "%d,%d\n", s + i, v + i
  }'
}

# 组装请求 JSON：$1=start $2=end $3=CSV 内容（生成的 CSV 只含数字/逗号/换行）
payload() {
  awk -v m="$METER" -v s="$1" -v e="$2" -v c="$3" 'BEGIN{
    gsub(/\n/, "\\n", c)
    printf "{\"meter_id\":\"%s\",\"start\":%s,\"end\":%s,\"csv\":\"%s\"}", m, s, e, c
  }'
}

post_archive() {  # $1=start $2=end $3=CSV
  curl -sS -X POST "$BASE_URL/archive" \
    -H 'Content-Type: application/json' \
    -d "$(payload "$1" "$2" "$3")" | pretty
}

get_rows() {  # $1=start $2=end
  curl -sS "$BASE_URL/archive?meter_id=$METER&start=$1&end=$2" | pretty
}

step "1/7 建档：[T0, T0+1500) 共 1500 行 -> 应写入 3 个块（512/512/476）"
post_archive "$T0" "$((T0+1500))" "$(make_csv "$T0" 1500 100000)"

step "2/7 块元数据：每块记录首尾采样秒，可独立解压"
curl -sS "$BASE_URL/archive/blocks?meter_id=$METER" | pretty

step "3/7 查询 [T0+500, T0+505)：只解压 1 个相交块（blocks_read=1）"
get_rows "$((T0+500))" "$((T0+505))"

step "4/7 跨块更正 [T0+400, T0+700)：新值 555000 起；重写 2 个旧块，段外 724 行保留"
post_archive "$((T0+400))" "$((T0+700))" "$(make_csv "$((T0+400))" 300 555000)"

step "5/7 更正后块元数据：第 3 块未被重写（id 与首尾秒不变）"
curl -sS "$BASE_URL/archive/blocks?meter_id=$METER" | pretty

step "6/7 清空 [T0+100, T0+200)：提交只有表头的 CSV"
post_archive "$((T0+100))" "$((T0+200))" "ts,count"

step "7/7 复核查询"
echo "-- [T0+98, T0+103)：只剩 98、99 两行（100 起已清空）"
get_rows "$((T0+98))" "$((T0+103))"
echo "-- [T0+398, T0+402)：399 是旧值 100399，400 起是更正值 555000"
get_rows "$((T0+398))" "$((T0+402))"
echo "-- 全天行数：1500 - 100（清空）= 1400"
get_rows "$T0" "$((T0+1500))" | { command -v jq >/dev/null 2>&1 && jq '{row_count, blocks_read}' || grep -o '"row_count":[0-9]*'; }
