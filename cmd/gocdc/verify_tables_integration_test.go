//go:build integration

// 启动对账建表集成测试（2026-11-11）：目标缺表 + ddl_sync=true → verifyTargetTables
// 按源结构自动建表；ddl_sync=false / ignore 策略 / 目标库缺失 / 挂起审批表各分支
// 维持原语义。触库路径必须容器验证（VERIFY 教训：Windows 单测不触库不算验证过）。
//
//	go test ./cmd/gocdc/ -tags=integration -count=1 -run TestIntegrationVerify
//
// 镜像默认 mysql:8，可用 MYSQL_IMAGE 覆盖。
package main

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

	"gocdc/internal/config"
	"gocdc/internal/pipeline"
	"gocdc/internal/schema"
	mysqlsink "gocdc/internal/sink/mysql"
)

// TestIntegrationVerifyTargetTablesAutoCreate 启动核对全分支（建表/拒建/fail-fast/挂起跳过）。
func TestIntegrationVerifyTargetTablesAutoCreate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ctr, err := mysql.Run(ctx, envOrCmd("MYSQL_IMAGE", "mysql:8"),
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
	admin, err := sql.Open("mysql", fmt.Sprintf("root:test@tcp(%s:%s)/?multiStatements=true", host, port.Port()))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	waitPingCmd(t, ctx, admin)

	// 源侧两组库：srcdb（auto 策略：t_auto 可建、t_pending 挂起跳过、t_nosync 反
	// DDLSync=false 分支）、srcdb2（ignore 策略：t_ignore 维持 fail-fast）。
	// 目标侧只有 dstdb（预建库）——ghostdb 用于缺库 fail-fast 分支。
	if _, err := admin.ExecContext(ctx, `
CREATE DATABASE srcdb;
CREATE TABLE srcdb.t_auto (
  id bigint NOT NULL,
  name varchar(64) NOT NULL DEFAULT '',
  PRIMARY KEY (id),
  UNIQUE KEY uq_name (name)
) ENGINE=InnoDB COMMENT='auto table';
CREATE TABLE srcdb.t_pending LIKE srcdb.t_auto;
CREATE TABLE srcdb.t_nosync LIKE srcdb.t_auto;
CREATE DATABASE srcdb2;
CREATE TABLE srcdb2.t_ignore LIKE srcdb.t_auto;`); err != nil {
		t.Fatal(err)
	}

	srcDSN := fmt.Sprintf("root:test@tcp(%s:%s)/", host, port.Port())
	srcDB, err := sql.Open("mysql", srcDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()
	msink, err := mysqlsink.New(mysqlsink.Options{
		Addr: host + ":" + port.Port(), User: "root", Password: "test",
		MaxOpenConns: 5, TxChunkRows: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer msink.Close(ctx)

	// group 规则组构造（组形态：mysql sink + 显式 target_database）。
	group := func(schemaName, policy string, tables ...string) config.Rule {
		g := config.Rule{
			Schema: schemaName, DDLPolicy: policy,
			Sinks:          &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
			TargetDatabase: "dstdb",
		}
		for _, tb := range tables {
			g.Tables = append(g.Tables, config.TableRule{Table: tb, OrderByColumn: "id", DoStock: stockPtr(true)})
		}
		return g
	}
	specFor := func(rules []config.Rule, ddlSync bool, globalTD string) *config.SourceSpec {
		return &config.SourceSpec{
			ID: "s1",
			Source: config.Source{
				Addr: host + ":" + port.Port(), User: "root", Password: "test",
				Charset: "utf8mb4", Flavor: "mysql", Timezone: "Asia/Shanghai",
			},
			Targets: config.Targets{MySQL: config.MySQLOptions{
				Enabled: true, Addr: host + ":" + port.Port(), User: "root", Password: "test",
				Charset: "utf8mb4", MaxOpenConns: 5, TxChunkRows: 2, DDLSync: ddlSync, TargetDatabase: globalTD,
			}},
			Rules: rules,
		}
	}
	runVerify := func(t *testing.T, spec *config.SourceSpec, pending map[string]bool) error {
		t.Helper()
		rules, err := pipeline.CompileRules(spec.Rules)
		if err != nil {
			t.Fatal(err)
		}
		sm := schema.NewManager(schema.NewMySQLLoader(srcDB))
		return verifyTargetTables(ctx, srcDB, sm, rules, spec, msink, pending)
	}

	// ── 1) auto + ddl_sync=true：t_auto 缺表 → 自动建表并核对通过；t_pending
	//      挂起审批 → 跳过建表、错误降级告警（重放停回审批）。──
	err = runVerify(t, specFor([]config.Rule{group("srcdb", "auto", "t_auto", "t_pending")}, true, ""), map[string]bool{"srcdb.t_pending": true})
	if err != nil {
		t.Fatalf("auto 策略应建表并核对通过（挂起表降级告警）: %v", err)
	}
	if ok, err := mysqlsink.TableExists(ctx, msink.DB(), "dstdb", "t_auto"); err != nil || !ok {
		t.Fatalf("t_auto 应已自动建表: ok=%v err=%v", ok, err)
	}
	if ok, err := mysqlsink.TableExists(ctx, msink.DB(), "dstdb", "t_pending"); err != nil || ok {
		t.Fatalf("挂起审批表不得自动建表: ok=%v err=%v", ok, err)
	}
	// 建出的表结构与源一致（含唯一键）——核对器已在 runVerify 内全量通过，此处
	// 抽查唯一键存在。
	var uqCount int
	if err := msink.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.STATISTICS
		 WHERE TABLE_SCHEMA='dstdb' AND TABLE_NAME='t_auto' AND INDEX_NAME='uq_name'`).Scan(&uqCount); err != nil || uqCount == 0 {
		t.Fatalf("建表应带源端唯一键 uq_name: n=%d err=%v", uqCount, err)
	}

	// ── 2) 幂等：重跑（装配监督退避重试场景）→ 无错误、不重复建。──
	if err := runVerify(t, specFor([]config.Rule{group("srcdb", "auto", "t_auto")}, true, ""), nil); err != nil {
		t.Fatalf("重跑应幂等通过: %v", err)
	}

	// ── 3) ddl_sync=false：缺表维持 fail-fast（diff/apply 未装配）。
	//    返回值=汇总错误（明细行走 stderr，见上方「t_nosync 不存在」输出）。──
	err = runVerify(t, specFor([]config.Rule{group("srcdb", "auto", "t_nosync")}, false, ""), nil)
	if err == nil || !strings.Contains(err.Error(), "核对未通过") {
		t.Fatalf("ddl_sync=false 缺表应 fail-fast: %v", err)
	}

	// ── 4) ignore 策略：目标结构另有管理人，缺表维持 fail-fast。──
	err = runVerify(t, specFor([]config.Rule{group("srcdb2", "ignore", "t_ignore")}, true, ""), nil)
	if err == nil || !strings.Contains(err.Error(), "核对未通过") {
		t.Fatalf("ignore 策略缺表应 fail-fast: %v", err)
	}

	// ── 5) 目标库缺失：库级不自动建（库须预建），ensure 建表失败进错误清单。
	//    组级 target_database 清空让全局 ghostdb 接管（解析序：表级>组级>全局）。──
	ghostGroup := group("srcdb", "auto", "t_auto")
	ghostGroup.TargetDatabase = ""
	err = runVerify(t, specFor([]config.Rule{ghostGroup}, true, "ghostdb"), nil)
	if err == nil || !strings.Contains(err.Error(), "核对未通过") {
		t.Fatalf("目标库缺失应 fail-fast: %v", err)
	}
}

// TestIntegrationPreflightCoreAutoCreate 活体预检缺表分支：ddl_sync=true → warning
// 放行（Web 加表不再被「目标表须预建」阻断）；ddl_sync=false → 维持 422 语义。
func TestIntegrationPreflightCoreAutoCreate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ctr, err := mysql.Run(ctx, envOrCmd("MYSQL_IMAGE", "mysql:8"),
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
	admin, err := sql.Open("mysql", fmt.Sprintf("root:test@tcp(%s:%s)/?multiStatements=true", host, port.Port()))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	waitPingCmd(t, ctx, admin)
	if _, err := admin.ExecContext(ctx, `
CREATE DATABASE srcdb;
CREATE TABLE srcdb.t_new (
  id bigint NOT NULL,
  name varchar(64) NOT NULL DEFAULT '',
  PRIMARY KEY (id)
) ENGINE=InnoDB;`); err != nil {
		t.Fatal(err)
	}

	mkSpec := func(ddlSync bool, td string) *config.SourceSpec {
		return &config.SourceSpec{
			ID: "s1",
			Source: config.Source{
				Addr: host + ":" + port.Port(), User: "root", Password: "test",
				Charset: "utf8mb4", Flavor: "mysql", Timezone: "Asia/Shanghai",
			},
			Targets: config.Targets{MySQL: config.MySQLOptions{
				Enabled: true, Addr: host + ":" + port.Port(), User: "root", Password: "test",
				Charset: "utf8mb4", MaxOpenConns: 5, TxChunkRows: 2, DDLSync: ddlSync, TargetDatabase: td,
			}},
		}
	}
	oldRules := []config.Rule{{
		Schema: "srcdb", DDLPolicy: "auto",
		Sinks:          &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
		TargetDatabase: "dstdb",
		Tables:         []config.TableRule{{Table: "t_old", OrderByColumn: "id", DoStock: stockPtr(true)}},
	}}
	withNew := func(tb config.TableRule) []config.Rule {
		rules := append([]config.Rule(nil), oldRules...)
		rules[0].Tables = append(rules[0].Tables, tb)
		return rules
	}
	// withoutGroupTD 组级 target_database 清空版（缺库分支用：让全局接管）。
	withoutGroupTD := func(tb config.TableRule) []config.Rule {
		rules := withNew(tb)
		rules[0].TargetDatabase = ""
		return rules
	}

	// ── 1) ddl_sync=true + 缺目标表 → warning 放行（自动建表预告）。──
	report := preflightRulesCore(ctx, mkSpec(true, ""), oldRules,
		withNew(config.TableRule{Table: "t_new", OrderByColumn: "id", DoStock: stockPtr(true)}))
	if len(report.Errors) != 0 {
		t.Fatalf("ddl_sync=true 缺表应放行: %+v", report)
	}
	if len(report.Warnings) == 0 || !strings.Contains(strings.Join(report.Warnings, "\n"), "自动建表") {
		t.Fatalf("应有自动建表预告 warning: %+v", report)
	}

	// ── 2) do_stock=false 的缺表预告必须声明「历史数据不回填」。──
	report = preflightRulesCore(ctx, mkSpec(true, ""), oldRules,
		withNew(config.TableRule{Table: "t_new", DoStock: stockPtr(false)}))
	if len(report.Errors) != 0 || !strings.Contains(strings.Join(report.Warnings, "\n"), "历史数据不回填") {
		t.Fatalf("do_stock=false 预告应声明不回填: %+v", report)
	}

	// ── 3) 目标库不存在 → 报错（库须预建；组级清空让全局 ghostdb 接管）。──
	report = preflightRulesCore(ctx, mkSpec(true, "ghostdb"), oldRules,
		withoutGroupTD(config.TableRule{Table: "t_new", OrderByColumn: "id", DoStock: stockPtr(true)}))
	if len(report.Errors) == 0 || !strings.Contains(report.Errors[0], "目标库") {
		t.Fatalf("目标库缺失应报错: %+v", report)
	}

	// ── 4) ddl_sync=false → 维持原语义：缺表 = error。──
	report = preflightRulesCore(ctx, mkSpec(false, ""), oldRules,
		withNew(config.TableRule{Table: "t_new", OrderByColumn: "id", DoStock: stockPtr(true)}))
	if len(report.Errors) == 0 || !strings.Contains(report.Errors[0], "不存在") {
		t.Fatalf("ddl_sync=false 缺表应报错: %+v", report)
	}
}

func envOrCmd(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func waitPingCmd(t *testing.T, ctx context.Context, db *sql.DB) {
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
