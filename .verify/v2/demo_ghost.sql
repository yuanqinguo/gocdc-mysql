-- 第二幕：白名单外表（shop.ghost_demo 不在 rules.tables 里）→ 建表/写入/删表都应被过滤
CREATE TABLE shop.ghost_demo (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(32) NULL, PRIMARY KEY(id));
INSERT INTO shop.ghost_demo (v) VALUES ('should-not-sync-1'),('should-not-sync-2');
