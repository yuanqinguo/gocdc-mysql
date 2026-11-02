-- 多源演示 · 源 B（用户中心）：msb.users
CREATE DATABASE IF NOT EXISTS msb CHARACTER SET utf8mb4;
CREATE TABLE msb.users (
  id BIGINT NOT NULL AUTO_INCREMENT,
  name VARCHAR(64) NULL,
  phone VARCHAR(20) NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO msb.users (name, phone) VALUES ('demo-user-1', '13900000001'), ('demo-user-2', NULL);
