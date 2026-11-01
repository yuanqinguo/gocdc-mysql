INSERT INTO types.all_types (c_tiny,c_small,c_med,c_int,c_big,c_ubig,c_dec,c_float,c_double,c_bit,c_char,c_varchar,c_text,c_blob,c_varbin,c_dt,c_ts,c_date,c_time,c_year,c_enum,c_set,c_json) VALUES
(-128,-32768,-8388608,-2147483648,-9223372036854775808,18446744073709551615,99999999.9999,3.14,2.718281828459045,b'10101010','char10','emoji🎉中文','长文本text内容...',_binary'rawblob01',UNHEX('8A2BFF'),'2026-10-29 12:34:56.789','2026-10-29 08:00:00','2026-02-28','23:59:59',2026,'b','x,z','{"nested":{"arr":[1,2,"三"],"t":true},"n":null}'),
(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
(127,32767,8388607,2147483647,9223372036854775807,0,-99999999.9999,-1.5e10,1e-15,b'0','a','x','y','z',UNHEX('00'),'1000-01-01 00:00:00.000','2038-01-01 00:00:00','9999-12-31','-838:59:59',1901,'c','x,y,z','[]');
INSERT INTO biz2.events (kind,payload) VALUES ('incr','增量第1条'),('incr','增量第2条');
UPDATE types.all_types SET c_varchar='更新后的值✅', c_dec=-1.0001, c_ubig=1, c_json='{"changed":true}' WHERE id=1;
DELETE FROM types.all_types WHERE id=2;
UPDATE shop.orders SET id=99999 WHERE id=5;
INSERT INTO shop.order_items VALUES (777,1,'新商品A',10),(777,2,'新商品B',20);
DELETE FROM shop.order_items WHERE order_id=777 AND item_no=1;
