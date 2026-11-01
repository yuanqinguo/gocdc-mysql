-- 演示目标库初始化：与 init-src.sql 的 demo.orders 同构
-- （05 §5.1：目标表须预先存在——同名同表、列一一对应；DDL 自动同步仅
--   处理增量期的结构漂移，不承担首次建表）
CREATE DATABASE IF NOT EXISTS demo;
USE demo;
CREATE TABLE IF NOT EXISTS orders (
  id BIGINT NOT NULL AUTO_INCREMENT,
  user_id BIGINT NOT NULL,
  amount DECIMAL(12,2) NOT NULL DEFAULT 0,
  remark VARCHAR(255) NULL,
  phone VARCHAR(20) NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
