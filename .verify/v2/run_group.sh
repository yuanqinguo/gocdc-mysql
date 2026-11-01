#!/bin/bash
# L3 组执行器：run_group.sh <NN_name> —— 跑单组并落 /tmp/v2-gNN.out
# 用法经 Git Bash: MSYS_NO_PATHCONV=1 wsl -d Ubuntu-22.04 -- bash .../run_group.sh 10_boot
G=$1
V2="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NN=$(echo "$G" | grep -oE '^[0-9]+')
OUT=/tmp/v2-g${NN}.out
bash "$V2/$G.sh" > >(tee "$OUT") 2>&1
EXIT=$?
echo "GROUP_EXIT=$EXIT (output: $OUT)"
exit $EXIT
