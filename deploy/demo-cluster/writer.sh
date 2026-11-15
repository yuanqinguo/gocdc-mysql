#!/usr/bin/env bash
# 每 3 秒往两个源库制造变更：INSERT / UPDATE / DELETE / upsert 全覆盖。
set -u

SRC_A=mysql-src-a
SRC_B=mysql-src-b
PW=123456

q() { mysql -h "$1" -uroot -p"$PW" "$2" -N -B -e "$3" 2>/dev/null; }

echo "[writer] waiting for source tables ..."
until q "$SRC_A" msa "SELECT 1 FROM products LIMIT 1" >/dev/null; do sleep 2; done
until q "$SRC_B" msb "SELECT 1 FROM user_profiles LIMIT 1" >/dev/null; do sleep 2; done
echo "[writer] sources ready, ticking every 3s"

i=0
while true; do
  i=$((i + 1))

  # 源 A：新增订单 + 明细
  q "$SRC_A" msa "
    INSERT INTO orders (user_id, amount, status, remark)
      VALUES (100 + ($i % 50), ROUND(10 + RAND() * 90, 2), 'NEW', CONCAT('tick-', $i));
    SET @oid := LAST_INSERT_ID();
    INSERT INTO order_items (order_id, sku, qty, price)
      VALUES (@oid, CONCAT('SKU-', LPAD($i % 5, 3, '0')), 1 + ($i % 3), ROUND(5 + RAND() * 50, 2));
  " || echo "[writer] src-a insert failed at tick $i"

  # 源 A：状态流转 + 库存增减
  q "$SRC_A" msa "
    UPDATE orders SET status = 'PAID' WHERE status = 'NEW' ORDER BY id LIMIT 1;
    INSERT INTO products (sku, title, price, stock)
      VALUES (CONCAT('SKU-DYN-', $i), CONCAT('动态商品 ', $i), ROUND(1 + RAND() * 20, 2), 10)
      ON DUPLICATE KEY UPDATE stock = stock + 1, updated_at = CURRENT_TIMESTAMP(3);
    UPDATE products SET stock = stock - 1
      WHERE sku = CONCAT('SKU-', LPAD($i % 5, 3, '0')) AND stock > 0 LIMIT 1;
  " || echo "[writer] src-a update failed at tick $i"

  # 每 5 拍删除一条动态明细，验证 DELETE 同步
  if [ $((i % 5)) -eq 0 ]; then
    q "$SRC_A" msa "DELETE FROM order_items WHERE id > 20 ORDER BY id LIMIT 1;" \
      || echo "[writer] src-a delete failed at tick $i"
  fi

  # 源 B：新增用户 + 地址 + 档案 upsert + 等级变更
  q "$SRC_B" msb "
    INSERT INTO users (name, phone, email, level)
      VALUES (CONCAT('user-', $i), CONCAT('139', LPAD($i, 8, '0')), CONCAT('u', $i, '@example.com'), $i % 5);
    SET @uid := LAST_INSERT_ID();
    INSERT INTO addresses (user_id, city, detail)
      VALUES (@uid, ELT(1 + ($i % 4), '北京', '上海', '广州', '深圳'), CONCAT('街道 ', $i, ' 号'));
    INSERT INTO user_profiles (user_id, nickname, bio)
      VALUES (@uid, CONCAT('nick-', $i), CONCAT('bio-', $i))
      ON DUPLICATE KEY UPDATE nickname = VALUES(nickname), bio = VALUES(bio);
    UPDATE users SET level = level + 1
      WHERE id = (SELECT id FROM (SELECT id FROM users ORDER BY RAND() LIMIT 1) t);
  " || echo "[writer] src-b write failed at tick $i"

  echo "[writer] tick $i at $(date '+%F %T')"
  sleep 3
done
