#!/bin/bash
# dbg_inspect_g20log.sh — 检查组20失败 run 的 node1.log：B2 窗口（workload1 后 ~10s）
# 找 warn/error/retry/退避 痕迹，以及 types 相关事件的处理时间线
LOG=/tmp/v2/node1.log
[ -f "$LOG" ] || { echo "log 不存在"; exit 1; }
echo "=== 文件信息 ==="; ls -la $LOG
echo "=== warn/error/重试/退避 行（全部）==="
grep -anE 'WARN|ERROR|warn|error|重试|退避|backoff|retry|failed|失败|拒绝' "$LOG" | head -40
echo "=== types/all_types 相关行 ==="
grep -an 'all_types' "$LOG" | head -20
echo "=== 时间线头尾 ==="
head -3 "$LOG"; echo ...; tail -5 "$LOG"
