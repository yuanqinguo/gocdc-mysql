//go:build integration

// 集成测试：需要 Docker。运行：
//
//	go test ./internal/sink/mysql/ -tags=integration -count=1
//
// 镜像默认 mysql:8，可用 MYSQL_IMAGE 覆盖。
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"gocdc/internal/model"
	"gocdc/internal/schema"
)

func TestIntegrationMySQLSink(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	image := envOr("MYSQL_IMAGE", "mysql:8")
	ctr, err := mysql.Run(ctx, image,
		mysql.WithDatabase("dstdb"), mysql.WithUsername("root"), mysql.WithPassword("test"))
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
	adminDSN := fmt.Sprintf("root:test@tcp(%s:%s)/", host, port.Port())
	admin, err := sql.Open("mysql", adminDSN+"?multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	waitPing(t, ctx, admin)

	// 目标表：联合主键（覆盖多列谓词场景）
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE IF NOT EXISTS dstdb;
CREATE TABLE dstdb.t_dst (
  tenant varchar(32) NOT NULL,
  id     bigint       NOT NULL,
  v      varchar(64)  NOT NULL DEFAULT '',
  amount decimal(10,2) NULL,
  ext    json          NULL,
  PRIMARY KEY (tenant, id)
) ENGINE=InnoDB`); err != nil {
		t.Fatal(err)
	}

	sink, err := New(Options{
		Addr: host + ":" + port.Port(), User: "root", Password: "test",
		MaxOpenConns: 5, TxChunkRows: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close(ctx)
	if err := sink.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	row := func(tenant string, id int64, v string) *model.Event {
		return &model.Event{
			Type: model.EventInsert, Database: "dstdb", Table: "t_dst",
			Pk: map[string]any{"tenant": tenant, "id": id},
			After: map[string]any{
				"tenant": tenant, "id": id, "v": v,
				"amount": "12.34", "ext": model.RawMessage(`{"k":1}`),
			},
		}
	}

	// 1) 批量 INSERT（chunk=2 → 两条多行 upsert）+ 幂等重放
	batch := []*model.Event{row("a", 1, "v1"), row("a", 2, "v2"), row("b", 1, "v3")}
	if err := sink.Send(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if err := sink.Send(ctx, batch); err != nil { // at-least-once 重放
		t.Fatal(err)
	}
	assertCount(t, ctx, admin, "SELECT COUNT(*) FROM dstdb.t_dst", 3)
	assertCount(t, ctx, admin, "SELECT COUNT(*) FROM dstdb.t_dst WHERE tenant='a' AND id=1 AND v='v1' AND amount='12.34'", 1)
	// JSON 列按文本落库且无损
	assertCount(t, ctx, admin, `SELECT COUNT(*) FROM dstdb.t_dst WHERE CAST(JSON_EXTRACT(ext,'$.k') AS CHAR)='1'`, 3)

	// 2) UPDATE（主键不变）→ upsert 生效
	upd := &model.Event{
		Type: model.EventUpdate, Database: "dstdb", Table: "t_dst",
		Pk:     map[string]any{"tenant": "a", "id": int64(1)},
		Before: map[string]any{"tenant": "a", "id": int64(1), "v": "v1"},
		After:  map[string]any{"tenant": "a", "id": int64(1), "v": "v1x", "amount": "99.99", "ext": model.RawMessage(`{"k":2}`)},
	}
	if err := sink.Send(ctx, []*model.Event{upd}); err != nil {
		t.Fatal(err)
	}
	assertCount(t, ctx, admin, "SELECT COUNT(*) FROM dstdb.t_dst WHERE tenant='a' AND id=1 AND v='v1x'", 1)

	// 3) 主键变更 → 同事务 delete(旧)+upsert(新)，旧行不残留
	pkChg := &model.Event{
		Type: model.EventUpdate, Database: "dstdb", Table: "t_dst",
		Pk:     map[string]any{"tenant": "b", "id": int64(2)},
		Before: map[string]any{"tenant": "b", "id": int64(1), "v": "v3"},
		After:  map[string]any{"tenant": "b", "id": int64(2), "v": "v3", "amount": "12.34", "ext": model.RawMessage(`{"k":1}`)},
	}
	if err := sink.Send(ctx, []*model.Event{pkChg}); err != nil {
		t.Fatal(err)
	}
	assertCount(t, ctx, admin, "SELECT COUNT(*) FROM dstdb.t_dst WHERE tenant='b'", 1)
	assertCount(t, ctx, admin, "SELECT COUNT(*) FROM dstdb.t_dst WHERE tenant='b' AND id=2", 1)

	// 4) 联合主键批量 DELETE（多列谓词）
	del := func(tenant string, id int64) *model.Event {
		return &model.Event{
			Type: model.EventDelete, Database: "dstdb", Table: "t_dst",
			Pk:     map[string]any{"tenant": tenant, "id": id},
			Before: map[string]any{"tenant": tenant, "id": id, "v": ""},
		}
	}
	if err := sink.Send(ctx, []*model.Event{del("a", 1), del("a", 2)}); err != nil {
		t.Fatal(err)
	}
	assertCount(t, ctx, admin, "SELECT COUNT(*) FROM dstdb.t_dst", 1)

	// 5) 目标结构核对：缺列 / exclude NOT NULL / 无主键寻址
	badMeta := &model.TableMeta{Schema: "dstdb", Table: "t_dst", Columns: []model.Column{
		{Name: "tenant", Type: model.TypeVarchar}, {Name: "id", Type: model.TypeBigint},
		{Name: "ghost_col", Type: model.TypeInt},
	}}
	res := VerifyTargetTable(ctx, sink.DB(), "dstdb", badMeta, nil, nil, "")
	if res.OK() {
		t.Fatalf("缺列应 fail-fast: %+v", res)
	}
	t.Logf("缺列报错: %v", res.Errors)

	// 6) 唯一索引必须与源表匹配：目标额外的严格唯一键会让 ON DUPLICATE KEY
	// UPDATE 把不同主键的行折叠（content_type/public_course_lessons 实案回归）。
	if _, err := admin.ExecContext(ctx, `
CREATE DATABASE srcdb;
CREATE TABLE srcdb.t_uq (
  id bigint NOT NULL,
  tenant bigint NOT NULL,
  name varchar(64) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_source (tenant, name)
) ENGINE=InnoDB;
CREATE TABLE dstdb.t_uq (
  id bigint NOT NULL,
  tenant bigint NOT NULL,
  name varchar(64) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_too_strict (name)
) ENGINE=InnoDB;`); err != nil {
		t.Fatal(err)
	}
	uqMeta, err := schema.NewMySQLLoader(admin).Load(ctx, "srcdb", "t_uq")
	if err != nil {
		t.Fatal(err)
	}
	res = VerifyTargetTable(ctx, sink.DB(), "dstdb", uqMeta, nil, nil, "")
	if res.OK() || !strings.Contains(strings.Join(res.Errors, "\n"), "ON DUPLICATE KEY UPDATE") {
		t.Fatalf("目标额外唯一键必须 fail-fast，并提示行折叠风险: %+v", res)
	}

	// 仅索引名不同、列与顺序一致时允许启动。
	if _, err := admin.ExecContext(ctx, `
DROP TABLE dstdb.t_uq;
CREATE TABLE dstdb.t_uq (
  id bigint NOT NULL,
  tenant bigint NOT NULL,
  name varchar(64) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_target_name (tenant, name)
) ENGINE=InnoDB;`); err != nil {
		t.Fatal(err)
	}
	res = VerifyTargetTable(ctx, sink.DB(), "dstdb", uqMeta, nil, nil, "")
	if !res.OK() {
		t.Fatalf("同列序仅索引名不同时应通过: %+v", res)
	}
}

func waitPing(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		if err := db.PingContext(ctx); err == nil {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("MySQL 未就绪: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func assertCount(t *testing.T, ctx context.Context, db *sql.DB, q string, want int64) {
	t.Helper()
	var n int64
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	if n != want {
		t.Fatalf("%s → %d, want %d", q, n, want)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
