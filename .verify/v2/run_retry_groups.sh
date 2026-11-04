#!/bin/bash
# L3 补跑：H2/I3 疑似 flake 复验 + 被残留锁挡掉的 70/80/90 三组
# 顺序：20 → 65 → 70 → 80 → 90（90 建 verify-single 第二集群，必须最后）
V2="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FAILED=()
for g in 20_data 65_mq 70_adv 80_exceptions 90_hiprio; do
  echo "════════ RETRY GROUP $g start $(date '+%F %T') ════════"
  if bash "$V2/run_group.sh" "$g"; then :; else FAILED+=("$g"); fi
done
echo "════════ RETRY TALLY $(date '+%F %T') ════════"
bash "$V2/tally.sh"
echo "RETRY_FAILED_GROUPS=${FAILED[*]:-none}"
bash "$V2/archive_logs.sh" || true
