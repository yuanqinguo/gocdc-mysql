-- 首启前种子数据：B1 全量 stock 预插 + F4 do_stock=false 预插不同步
-- shop.orders 100 行（id=5 留给 B4 主键变更；user 12 的 phone 留给 B9 mask 验证）
INSERT INTO shop.orders (id,user_id,amount,remark)
SELECT n, 100+n, ROUND(RAND()*1000,2), CONCAT('seed-',n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<100) SELECT n FROM s) t;
UPDATE shop.orders SET amount=amount WHERE id=5;
-- shop.users 100 行（id=12 phone=13900000012 → B9 mask 后 139****0012）
INSERT INTO shop.users (id,name,phone,created)
SELECT n, CONCAT('user',n), CONCAT('1390000',LPAD(n,4,'0')), NOW() - INTERVAL n DAY FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<100) SELECT n FROM s) t;
UPDATE shop.users SET phone='13900000012' WHERE id=12;
-- shop.order_items 100 行（联合主键）
INSERT INTO shop.order_items (order_id,item_no,product,qty)
SELECT n, (n % 3)+1, CONCAT('prod-',n), (n % 7)+1 FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<100) SELECT n FROM s) t;
-- types.all_types 5 行简单值（workload1 的全类型极值行走增量阶段）
INSERT INTO types.all_types (c_tiny,c_small,c_med,c_int,c_big,c_ubig,c_dec,c_varchar,c_enum) VALUES
(1,2,3,4,5,6,10.5,'stock-1','a'),(-1,-2,-3,-4,-5,0,-1.5,'stock-2','b'),(NULL,NULL,NULL,NULL,NULL,NULL,NULL,'stock-3',NULL),(100,300,500,700,900,1200,3.25,'stock-4','c'),(0,0,0,0,0,0,0,'stock-5','a');
-- biz2.events 20 行（F4：do_stock=false → 启动后目标 biz2 必须仍 0 行）
INSERT INTO biz2.events (kind,payload)
SELECT 'seed', CONCAT('预插不同步-',n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<20) SELECT n FROM s) t;
