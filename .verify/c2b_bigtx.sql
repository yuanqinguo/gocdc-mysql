SET SESSION cte_max_recursion_depth=60000;
INSERT INTO shop.orders (user_id,amount,remark)
SELECT 8000+n, 2.00, CONCAT('c2b-',n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<50000) SELECT n FROM s) t;
