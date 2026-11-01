INSERT INTO types.all_types (c_varchar) VALUES (REPEAT('长',100));
SELECT id, CHAR_LENGTH(c_varchar) AS len, HEX(LEFT(c_varchar,3)) AS head FROM types.all_types WHERE CHAR_LENGTH(c_varchar)=100 ORDER BY id DESC LIMIT 1;
