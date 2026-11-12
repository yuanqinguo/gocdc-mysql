-- 演示目标库初始化：与 init-src.sql 的 demo.orders 同构（同名同表、列一一对应）。
-- 05 §5.1：目标库必须预建（库级不自动建）；目标表可预建（本文件方式），或在
-- targets.mysql.ddl_sync=true 时由启动对账按源结构自动建表（08 §2）——本演示
-- 选择预建；未预建时删掉下方 CREATE TABLE 亦可走自动建表路径。
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
