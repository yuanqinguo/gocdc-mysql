SET SESSION cte_max_recursion_depth=400000;
INSERT INTO shop.orders (user_id,amount,remark) SELECT 9000+n, 3.00, CONCAT('c2c-',n,'-',REPEAT('x',200)) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<300000) SELECT n FROM s) t;
