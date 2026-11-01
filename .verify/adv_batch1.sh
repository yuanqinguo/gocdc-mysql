-- ADV-01: biz2 库建无主键表（biz2 规则未配 order_by_column）
CREATE TABLE biz2.no_pk (id BIGINT NOT NULL, val VARCHAR(32) NULL);
INSERT INTO biz2.no_pk VALUES (1, 'x');
