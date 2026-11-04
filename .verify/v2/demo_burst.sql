-- 第三幕：单事务大事务（2000 行一次提交），观察概览页 lag 抬升→追赶→归零
DROP TABLE IF EXISTS shop.ghost_demo;
INSERT INTO cluster_e2e.tab (v)
SELECT CONCAT('burst-',n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<2000) SELECT n FROM s) t;
SELECT COUNT(*) AS burst_total FROM cluster_e2e.tab;
