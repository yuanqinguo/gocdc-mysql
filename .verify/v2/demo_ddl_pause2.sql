-- 第五幕：再来一次 pause DDL——shop.orders 加列（shop 组默认 pause 策略）
ALTER TABLE shop.orders ADD COLUMN demo_channel VARCHAR(32) NULL DEFAULT 'web';
