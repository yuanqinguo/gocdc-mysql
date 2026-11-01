TRUNCATE TABLE `edu.mall.admin`.users;
INSERT INTO `edu.mall.admin`.users (name,phone) VALUES ('dotdb-a','13900000001'),('dotdb-b','13900000002'),('dotdb-c',NULL);
SELECT COUNT(*) AS seeded FROM `edu.mall.admin`.users;
TRUNCATE TABLE `edu.mall.merge`.users;
