-- B10 同行 25 次高频 UPDATE（终值 25.50）+ B7 单事务 5000 行
UPDATE shop.orders SET amount=0.50, remark='b10' WHERE id=99999;
UPDATE shop.orders SET amount=1.50 WHERE id=99999;
UPDATE shop.orders SET amount=2.50 WHERE id=99999;
UPDATE shop.orders SET amount=3.50 WHERE id=99999;
UPDATE shop.orders SET amount=4.50 WHERE id=99999;
UPDATE shop.orders SET amount=5.50 WHERE id=99999;
UPDATE shop.orders SET amount=6.50 WHERE id=99999;
UPDATE shop.orders SET amount=7.50 WHERE id=99999;
UPDATE shop.orders SET amount=8.50 WHERE id=99999;
UPDATE shop.orders SET amount=9.50 WHERE id=99999;
UPDATE shop.orders SET amount=10.50 WHERE id=99999;
UPDATE shop.orders SET amount=11.50 WHERE id=99999;
UPDATE shop.orders SET amount=12.50 WHERE id=99999;
UPDATE shop.orders SET amount=13.50 WHERE id=99999;
UPDATE shop.orders SET amount=14.50 WHERE id=99999;
UPDATE shop.orders SET amount=15.50 WHERE id=99999;
UPDATE shop.orders SET amount=16.50 WHERE id=99999;
UPDATE shop.orders SET amount=17.50 WHERE id=99999;
UPDATE shop.orders SET amount=18.50 WHERE id=99999;
UPDATE shop.orders SET amount=19.50 WHERE id=99999;
UPDATE shop.orders SET amount=20.50 WHERE id=99999;
UPDATE shop.orders SET amount=21.50 WHERE id=99999;
UPDATE shop.orders SET amount=22.50 WHERE id=99999;
UPDATE shop.orders SET amount=23.50 WHERE id=99999;
UPDATE shop.orders SET amount=24.50 WHERE id=99999;
UPDATE shop.orders SET amount=25.50, remark='b10-final' WHERE id=99999;
-- B7: 5000 行单事务
INSERT INTO shop.orders (user_id,amount,remark)
SELECT 7000+n, 1.00, CONCAT('b7-',n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<5000) SELECT n FROM s) t;
SELECT COUNT(*) AS orders_total, (SELECT amount FROM shop.orders WHERE id=99999) AS b10_final;
