#!/bin/bash
echo "组名 | PASS | FAIL | 失败项"
total_p=0; total_f=0
for f in /tmp/v2-g10.out /tmp/v2-g20.out /tmp/v2-g30.out /tmp/v2-g40.out /tmp/v2-g45.out /tmp/v2-g50.out /tmp/v2-g55.out /tmp/v2-g60.out /tmp/v2-g65.out /tmp/v2-g70.out /tmp/v2-g80.out /tmp/v2-g90.out; do
  [ -f "$f" ] || continue
  line=$(grep -a '组完成' "$f" | tail -1)
  p=$(echo "$line" | grep -oE 'PASS=[0-9]+' | cut -d= -f2)
  fl=$(echo "$line" | grep -oE 'FAIL=[0-9]+' | cut -d= -f2)
  cases=$(echo "$line" | grep -oE '\|.*' | cut -d'|' -f2-)
  total_p=$((total_p+${p:-0})); total_f=$((total_f+${fl:-0}))
  name=$(basename "$f" .out)
  printf "%s | %s | %s | %s\n" "$name" "${p:-?}" "${fl:-?}" "${cases:--}"
done
echo "════ 总计: PASS=$total_p FAIL=$total_f ════"
