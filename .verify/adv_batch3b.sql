CREATE TABLE biz2.`order` (`id` BIGINT NOT NULL AUTO_INCREMENT, `key` VARCHAR(32) NOT NULL, `select` INT NULL, PRIMARY KEY (`id`));
INSERT INTO biz2.`order` (`key`, `select`) VALUES ('reserve', 1), ('words', 2);
INSERT INTO biz2.`order` (`key`) VALUES ('upd-reserve');
UPDATE biz2.`order` SET `select` = 99 WHERE `key` = 'reserve';
-- ADV-05: 64KB 超长行（c_text TEXT 类型）
INSERT INTO types.all_types (c_text) SELECT REPEAT('x', 65536);
-- ADV-06: 显式大 id
INSERT INTO biz2.events (id, kind, payload) VALUES (999999, 'big-id', 'explicit-id-row');
INSERT INTO biz2.events (kind, payload) VALUES ('after-bigid', 'still-works');
