-- 源 A（订单域）：msa.orders / msa.order_items / msa.products
CREATE DATABASE IF NOT EXISTS msa CHARACTER SET utf8mb4;

CREATE TABLE msa.orders (
  id BIGINT NOT NULL AUTO_INCREMENT,
  user_id BIGINT NOT NULL,
  amount DECIMAL(10,2) NOT NULL DEFAULT 0,
  status VARCHAR(16) NOT NULL DEFAULT 'NEW',
  remark VARCHAR(128) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_orders_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE msa.order_items (
  id BIGINT NOT NULL AUTO_INCREMENT,
  order_id BIGINT NOT NULL,
  sku VARCHAR(32) NOT NULL,
  qty INT NOT NULL DEFAULT 1,
  price DECIMAL(10,2) NOT NULL DEFAULT 0,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_items_order (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE msa.products (
  id BIGINT NOT NULL AUTO_INCREMENT,
  sku VARCHAR(32) NOT NULL,
  title VARCHAR(128) NOT NULL,
  price DECIMAL(10,2) NOT NULL DEFAULT 0,
  stock INT NOT NULL DEFAULT 0,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_products_sku (sku)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO msa.orders (user_id, amount, status, remark) VALUES
  (1, 19.90, 'NEW',  'seed-a-1'),
  (2, 29.90, 'PAID', 'seed-a-2'),
  (3, 39.90, 'NEW',  'seed-a-3'),
  (4, 49.90, 'DONE', 'seed-a-4'),
  (5, 59.90, 'NEW',  'seed-a-5'),
  (1, 69.90, 'PAID', 'seed-a-6');

INSERT INTO msa.order_items (order_id, sku, qty, price) VALUES
  (1, 'SKU-001', 1, 19.90), (2, 'SKU-002', 2, 9.95),
  (3, 'SKU-003', 1, 39.90), (4, 'SKU-004', 3, 16.63),
  (5, 'SKU-000', 1, 59.90), (6, 'SKU-001', 2, 34.95),
  (1, 'SKU-004', 1, 0.00), (2, 'SKU-002', 1, 9.95);

INSERT INTO msa.products (sku, title, price, stock) VALUES
  ('SKU-000', '种子商品 0', 59.90, 100),
  ('SKU-001', '种子商品 1', 19.90, 100),
  ('SKU-002', '种子商品 2',  9.95, 100),
  ('SKU-003', '种子商品 3', 39.90, 100),
  ('SKU-004', '种子商品 4', 16.63, 100);
