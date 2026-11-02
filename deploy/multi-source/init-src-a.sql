-- 多源演示 · 源 A（订单中心）：msa.orders
CREATE DATABASE IF NOT EXISTS msa CHARACTER SET utf8mb4;
CREATE TABLE msa.orders (
  id BIGINT NOT NULL AUTO_INCREMENT,
  val VARCHAR(64) NULL,
  amt DECIMAL(10,2) NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO msa.orders (val, amt) VALUES ('demo-seed-1', 1.11), ('demo-seed-2', 2.22);
