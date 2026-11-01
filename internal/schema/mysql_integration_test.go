//go:build integration

// 集成测试：需要 Docker。运行：
//
//	go test ./internal/schema/ -tags=integration -count=1
//
// 镜像默认 mysql:8.0（与 deploy/docker-compose.yml 一致），可用 MYSQL_IMAGE 覆盖。
package schema

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"gocdc/internal/model"
)

// startMySQL 启动一次性 MySQL 容器（root/test，库 gocdc_test）。
func startMySQL(t *testing.T, ctx context.Context, image string) *mysql.MySQLContainer {
	t.Helper()
	ctr, err := mysql.Run(ctx, image,
		mysql.WithDatabase("gocdc_test"),
		mysql.WithUsername("root"),
		mysql.WithPassword("test"),
	)
	if err != nil {
		t.Fatalf("启动 MySQL 容器: %v", err)
	}
	return ctr
}

// TestIntegrationMySQLLoader 容器 MySQL 建表 → information_schema 加载断言。
// 覆盖：联合主键、unsigned、decimal(65,30)、bit(12)、enum（含引号转义）、set、json、
// datetime(3)、charset、表/列注释、版本化 Refresh。
func TestIntegrationMySQLLoader(t *testing.T) {
	ctx := context.Background()
	image := envOr("MYSQL_IMAGE", "mysql:8.0")

	ctr := startMySQL(t, ctx, image)
	t.Cleanup(func() { _ = ctr.Terminate(ctx) })

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := ctr.MappedPort(ctx, "3306")
	if err != nil {
		t.Fatal(err)
	}
	dsn := "root:test@tcp(" + host + ":" + port.Port() + ")/gocdc_test?multiStatements=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// 容器首启初始化可能需数秒
	deadline := time.Now().Add(60 * time.Second)
	for {
		if err = db.PingContext(ctx); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("MySQL 未就绪: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	_, err = db.ExecContext(ctx, `
CREATE TABLE t_all (
  id          bigint unsigned NOT NULL,
  tenant      varchar(64)     NOT NULL,
  amount      decimal(65,30)  NOT NULL DEFAULT '0',
  flags       bit(12)         NOT NULL DEFAULT b'0',
  status      enum('active','locked','x''q') NOT NULL DEFAULT 'active',
  tags        set('a','b','c')         NULL,
  ext         json                     NULL,
  hit_at      datetime(3)              NULL,
  note        text                     NULL,
  secret      varbinary(128)           NULL,
  PRIMARY KEY (tenant, id),
  UNIQUE KEY uq_tenant_amount (tenant, amount)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='全类型样本表';`)
	if err != nil {
		t.Fatal(err)
	}

	m := NewManager(NewMySQLLoader(db))
	meta, err := m.Get(ctx, "gocdc_test", "t_all")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Version != 1 {
		t.Fatalf("首次版本应 1，实际 %d", meta.Version)
	}
	if meta.Comment != "全类型样本表" {
		t.Fatalf("表注释: %q", meta.Comment)
	}
	if len(meta.Columns) != 10 {
		t.Fatalf("列数: %d", len(meta.Columns))
	}
	if got, want := meta.PKColumns, []string{"tenant", "id"}; got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("联合主键序: %v", meta.PKColumns)
	}
	if !meta.UniqueIndexesLoaded || len(meta.UniqueIndexes) != 2 {
		t.Fatalf("唯一索引元数据: loaded=%v indexes=%+v", meta.UniqueIndexesLoaded, meta.UniqueIndexes)
	}
	if got := meta.UniqueIndexes[0]; got.Name != "PRIMARY" || len(got.Parts) != 2 || got.Parts[0].Column != "tenant" || got.Parts[1].Column != "id" {
		t.Fatalf("PRIMARY 索引序: %+v", got)
	}
	if got := meta.UniqueIndexes[1]; got.Name != "uq_tenant_amount" || len(got.Parts) != 2 || got.Parts[0].Column != "tenant" || got.Parts[1].Column != "amount" {
		t.Fatalf("二级唯一索引序: %+v", got)
	}
	assertCol := func(name, typ string, mut func(*model.Column)) {
		t.Helper()
		c, _ := meta.ColumnByName(name)
		if c == nil {
			t.Fatalf("缺列 %s", name)
		}
		if c.Type != typ {
			t.Fatalf("列 %s 类型=%s 期望 %s", name, c.Type, typ)
		}
		if wantPK := name == "tenant" || name == "id"; c.IsPK != wantPK {
			t.Fatalf("列 %s IsPK=%v 期望 %v", name, c.IsPK, wantPK)
		}
		if mut != nil {
			mut(c)
		}
	}
	assertCol("id", model.TypeBigint, func(c *model.Column) {
		if !c.Unsigned {
			t.Fatal("id 应为 unsigned")
		}
	})
	assertCol("tenant", model.TypeVarchar, func(c *model.Column) {
		if c.Charset != "utf8mb4" || c.Precision != 64 {
			t.Fatalf("tenant charset=%q precision=%d", c.Charset, c.Precision)
		}
	})
	assertCol("amount", model.TypeDecimal, func(c *model.Column) {
		if c.Precision != 65 || c.Scale != 30 {
			t.Fatalf("amount precision=%d scale=%d", c.Precision, c.Scale)
		}
	})
	assertCol("flags", model.TypeBit, func(c *model.Column) {
		if c.Precision != 12 {
			t.Fatalf("bit 精度: %d", c.Precision)
		}
	})
	assertCol("status", model.TypeEnum, func(c *model.Column) {
		if len(c.EnumValues) != 3 || c.EnumValues[2] != "x'q" {
			t.Fatalf("enum 取值（含引号转义）: %#v", c.EnumValues)
		}
	})
	assertCol("tags", model.TypeSet, func(c *model.Column) {
		if len(c.SetValues) != 3 || c.SetValues[1] != "b" {
			t.Fatalf("set 取值: %#v", c.SetValues)
		}
	})
	assertCol("ext", model.TypeJSON, nil)
	assertCol("hit_at", model.TypeDatetime, func(c *model.Column) {
		if c.Precision != 3 {
			t.Fatalf("datetime fsp: %d", c.Precision)
		}
	})
	assertCol("secret", model.TypeVarbinary, func(c *model.Column) {
		if c.Charset != "" {
			t.Fatalf("二进制列不应有 charset: %q", c.Charset)
		}
	})

	// 版本化 Refresh：+1 且实例替换
	meta2, err := m.Refresh(ctx, "gocdc_test", "t_all")
	if err != nil {
		t.Fatal(err)
	}
	if meta2.Version != 2 || meta == meta2 {
		t.Fatalf("Refresh 应 +1 换实例: %d", meta2.Version)
	}
	// DDL 加列后刷新可见
	if _, err := db.ExecContext(ctx, `ALTER TABLE t_all ADD COLUMN extra_col int NULL`); err != nil {
		t.Fatal(err)
	}
	meta3, err := m.Refresh(ctx, "gocdc_test", "t_all")
	if err != nil {
		t.Fatal(err)
	}
	if meta3.Version != 3 || len(meta3.Columns) != 11 {
		t.Fatalf("加列后刷新: version=%d cols=%d", meta3.Version, len(meta3.Columns))
	}
	// 不存在的表
	if _, err := m.Get(ctx, "gocdc_test", "ghost"); !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("期望 ErrTableNotFound: %v", err)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
