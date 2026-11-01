#!/bin/bash
# debug2: 精确复现 70_adv.sh 中的单引号 sed 形态
V2="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
sed '/table: dying_table/a\      - {table: noltbl, do_stock: true, order_by_column: v}' \
  "$V2/body_main.yml" > /tmp/v2/test-sed2.yml 2>/tmp/v2/test-sed2.err
echo "exit=$?"
cat /tmp/v2/test-sed2.err
wc -c /tmp/v2/test-sed2.yml
# 也测 ADV-10 的 sed 形态
sed 's/auth_disabled: false/auth_disabled: true/' "$V2/body_main.yml" > /tmp/v2/test-auth.yml 2>/tmp/v2/test-auth.err
echo "auth exit=$?"
cat /tmp/v2/test-auth.err
wc -c /tmp/v2/test-auth.yml
