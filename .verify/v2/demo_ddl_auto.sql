-- 第一幕：auto 策略白名单 DDL（加列 + varchar 扩容），目标库应自动同步、无需审批
ALTER TABLE types.all_types ADD COLUMN demo_col VARCHAR(64) NULL;
ALTER TABLE rmqtest.msg MODIFY COLUMN note VARCHAR(500) NULL;
UPDATE types.all_types SET demo_col='auto-ddl-演示✅' WHERE id=1;
INSERT INTO rmqtest.msg (name,qty,amount,note) VALUES ('ddl-demo',1,9.99,'扩容验证-note列已到500宽度');
