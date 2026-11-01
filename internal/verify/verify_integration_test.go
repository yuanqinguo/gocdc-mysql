//go:build integration

// 集成测试：需要 Docker。运行：
//
//	go test ./internal/verify/ -tags=integration -count=1
//
// 单容器建源/目标双库（同 sink/mysql 集成模式），注入真实差异
// （1 DIFF + 1 MISS）断言对账结果——用真实链路钉死按列比对语义
// （SELECT 多列扫进单值会被 database/sql 拒绝）。
package verify

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func waitPing(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	for i := 0; i < 30; i++ {
		if err := db.PingContext(ctx); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("数据库 ping 超时")
		case <-time.After(time.Second):
		}
	}
}

func TestIntegrationCompareTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ctr, err := mysql.Run(ctx, envOr("MYSQL_IMAGE", "mysql:8"),
		mysql.WithDatabase("srcdb"), mysql.WithUsername("root"), mysql.WithPassword("test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := ctr.MappedPort(ctx, "3306")
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", fmt.Sprintf("root:test@tcp(%s:%s)/?multiStatements=true", host, port.Port()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	waitPing(t, ctx, db)

	// 源库 10 行；目标库同构表 9 行（少 1 行 MISS）且 id=3 的值不同（1 DIFF）。
	// srcdb 由容器初始化建立（mysql.WithDatabase）——这里只建目标库。
	if _, err := db.ExecContext(ctx, `
CREATE DATABASE IF NOT EXISTS dstdb;
CREATE TABLE srcdb.orders (
  id BIGINT NOT NULL AUTO_INCREMENT,
  v varchar(64) NOT NULL DEFAULT '',
  amount decimal(10,2) NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB;
CREATE TABLE dstdb.orders LIKE srcdb.orders;`); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 10; i++ {
		if _, err := db.ExecContext(ctx,
			"INSERT INTO srcdb.orders (id, v, amount) VALUES (?, ?, ?)",
			i, fmt.Sprintf("row-%d", i), fmt.Sprintf("%d.50", i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i <= 10; i++ {
		if i == 7 {
			continue // 目标缺行 → MISS
		}
		v := fmt.Sprintf("row-%d", i)
		if i == 3 {
			v = "drifted" // 值漂移 → DIFF
		}
		if _, err := db.ExecContext(ctx,
			"INSERT INTO dstdb.orders (id, v, amount) VALUES (?, ?, ?)",
			i, v, fmt.Sprintf("%d.50", i)); err != nil {
			t.Fatal(err)
		}
	}

	res, err := CompareTable(ctx, &Options{
		SrcDB: db, TgtDB: db, Schema: "srcdb", TargetDB: "dstdb",
		Table: "orders", PK: "id", Sample: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.SrcCount != 10 || res.TgtCount != 9 {
		t.Fatalf("count 不符: src=%d tgt=%d", res.SrcCount, res.TgtCount)
	}
	if res.Mismatch != 2 {
		t.Fatalf("应 2 处差异（1 DIFF + 1 MISS），实际 %d: %v", res.Mismatch, res.Diffs)
	}
	if res.Checked != 8 {
		t.Fatalf("应 8 行一致，实际 %d", res.Checked)
	}
	if res.Truncated {
		t.Fatal("差异 2 条不应截断")
	}
	foundDiff, foundMiss := false, false
	for _, d := range res.Diffs {
		switch {
		case len(d) > 4 && d[:4] == "DIFF":
			foundDiff = d[4:] != "" && contains(d, "pk=3")
		case len(d) > 4 && d[:4] == "MISS":
			foundMiss = contains(d, "pk=7")
		}
	}
	if !foundDiff || !foundMiss {
		t.Fatalf("差异明细应含 DIFF pk=3 与 MISS pk=7: %v", res.Diffs)
	}
	if res.OK() {
		t.Fatal("存在差异时 OK() 应为 false")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
