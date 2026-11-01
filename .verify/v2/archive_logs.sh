#!/bin/bash
# archive_logs.sh — 组结果留档到 .verify/v2/logs/（/tmp 隔夜即失，PLAYBOOK §7）
D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/logs"
mkdir -p "$D"
cp -a /tmp/v2-g*.out "$D"/ 2>/dev/null
ls -la "$D"
