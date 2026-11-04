#!/bin/bash
# L3 全量执行器：按 PLAYBOOK §3 顺序跑 14 组 → tally → archive
# 单组失败不中断（各组 wipe_all 起步，环境自恢复），末尾汇总 FAILED_GROUPS。
V2="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FAILED=()
for g in 10_boot 20_data 30_pos 35_ms_core 40_ha 45_ha_outage 46_ms_ha 50_ddl 55_op_pause 60_api 65_mq 70_adv 80_exceptions 90_hiprio; do
  echo "════════ GROUP $g start $(date '+%F %T') ════════"
  if bash "$V2/run_group.sh" "$g"; then :; else FAILED+=("$g"); fi
done
echo "════════ TALLY $(date '+%F %T') ════════"
bash "$V2/tally.sh"
echo "FAILED_GROUPS=${FAILED[*]:-none}"
bash "$V2/archive_logs.sh" || true
