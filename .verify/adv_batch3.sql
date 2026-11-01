CREATE TABLE biz2.`order` (`id` BIGINT NOT NULL AUTO_INCREMENT, `key` VARCHAR(32) NOT NULL, `select` INT NULL, PRIMARY KEY (`id`));
INSERT INTO biz2.`order` (`key`, `select`) VALUES ('reserve', 1), ('words', 2);
-- ADV-05: 64KB 超长行
INSERT INTO biz2.events (kind, payload) SELECT 'big64k', REPEAT('x', 65536);
-- ADV-06: 显式大 id（目标 AUTO_INCREMENT 漂移源）
INSERT INTO biz2.events (id, kind, payload) VALUES (999999, 'big-id', 'explicit-id-row');
