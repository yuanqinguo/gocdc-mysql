-- 第四幕：pause 策略 DDL（shop 组默认 pause）→ 管线挂起、位点冻结、等 Web 审批
-- 预期：/api/v1/ddl-events 出现 pending 事件；目标库暂不加列；approve 后自动在目标执行并恢复增量
ALTER TABLE shop.users ADD COLUMN demo_level INT NOT NULL DEFAULT 0;
