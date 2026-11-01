package pipeline

import "testing"

// ddlTarget 表引用解析（08 §3）：覆盖全部合法形态 + 带点库名场景
// （`edu.mall.admin`.users 的首个点不得当库名分隔符，否则解析成 edu/mall
// 被 want 静默丢弃，auto/pause 双策略失效）。
func TestDDlTarget(t *testing.T) {
	cases := []struct {
		name  string
		query string
		def   string
		sch   string
		tbl   string
	}{
		{"裸表", "ALTER TABLE users ADD COLUMN c INT", "defdb", "defdb", "users"},
		{"裸表含IF EXISTS", "DROP TABLE IF EXISTS users", "defdb", "defdb", "users"},
		{"无引号两段", "ALTER TABLE shop.users ADD COLUMN c INT", "defdb", "shop", "users"},
		{"反引号普通", "ALTER TABLE `shop`.`users` ADD COLUMN c INT", "defdb", "shop", "users"},
		{"反引号混合", "ALTER TABLE `shop`.users ADD COLUMN c INT", "defdb", "shop", "users"},
		// 引号在段中（裸库+反引号表）：db 段含分隔尾点，必须 TrimSuffix——
		// 否则 shop.`order` 解析成 schema="shop." 被 want 静默丢弃
		// （DDL 不挂起不同步无日志），保留字表名是核心场景
		{"裸库反引号表保留字", "CREATE TABLE shop.`order` (id BIGINT NOT NULL AUTO_INCREMENT, `key` INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /* xid */", "defdb", "shop", "order"},
		{"裸库反引号表普通名", "CREATE TABLE shop.`orderx` (id BIGINT NOT NULL AUTO_INCREMENT, v INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", "defdb", "shop", "orderx"},
		{"带点库名反引号表", "ALTER TABLE edu.mall.admin.`users` ADD COLUMN email VARCHAR(64) NULL", "defdb", "edu.mall.admin", "users"},
		{"带点库名反引号", "ALTER TABLE `edu.mall.admin`.users ADD COLUMN email VARCHAR(64) NULL", "defdb", "edu.mall.admin", "users"},
		{"带点库名双引号", `ALTER TABLE "edu.mall.admin".users ADD COLUMN c INT`, "defdb", "edu.mall.admin", "users"},
		{"带点库名CREATE", "CREATE TABLE `edu.mall.admin`.new (id INT)", "defdb", "edu.mall.admin", "new"},
		{"带点库名TRUNCATE", "TRUNCATE TABLE `edu.mall.admin`.users", "defdb", "edu.mall.admin", "users"},
		{"带点库名DROP", "DROP TABLE `edu.mall.admin`.`users`", "defdb", "edu.mall.admin", "users"},
		{"RENAME首表", "RENAME TABLE shop.tmp_log TO shop.tmp_log2", "defdb", "shop", "tmp_log"},
		{"小写alter", "alter table shop.users add column c int", "defdb", "shop", "users"},
		{"引号不闭合兜底", "ALTER TABLE `edu.mall.admin.users ADD COLUMN c INT", "defdb", "", ""},
		{"无引号三段非法兜底", "ALTER TABLE edu.mall.admin.users ADD COLUMN c INT", "defdb", "", ""},
		{"非DDL语句不匹配", "INSERT INTO shop.users VALUES (1)", "defdb", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sch, tbl := ddlTarget(c.query, c.def)
			if sch != c.sch || tbl != c.tbl {
				t.Fatalf("ddlTarget(%q) = (%q,%q)，期待 (%q,%q)", c.query, sch, tbl, c.sch, c.tbl)
			}
		})
	}
}
