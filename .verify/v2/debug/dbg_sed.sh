#!/bin/bash
# debug: 复现 70 组 sed 产空文件问题
V2="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "--- input file check:"
ls -la "$V2/body_main.yml"
file "$V2/body_main.yml" 2>/dev/null
echo "--- plain read test:"
wc -c < "$V2/body_main.yml" && echo READ_OK
echo "--- sed test:"
sed "/table: dying_table/a\\      - {table: noltbl, do_stock: true, order_by_column: v}" "$V2/body_main.yml" > /tmp/v2/test-sed.yml 2>/tmp/v2/test-sed.err
echo "sed exit=$?"
cat /tmp/v2/test-sed.err
wc -c /tmp/v2/test-sed.yml
grep -n "noltbl" /tmp/v2/test-sed.yml || echo "NO noltbl in output"
