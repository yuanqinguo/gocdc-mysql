package config

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestValidate_DottedSchemaAndTargetDB 库名允许点（源库库名形如
// edu.mall.admin / edu.mall.merge——多源库按 . 命名聚合）；表名/列名仍禁点。
func TestValidate_DottedSchemaAndTargetDB(t *testing.T) {
	dir := t.TempDir()
	mk := func(rulesYAML, targetDB string) string {
		raw := `cluster: c1
source:
  addr: "10.0.0.1:3306"
  user: repl
  password: x
  charset: utf8mb4
  flavor: mysql
  server_id: 1
  timezone: Asia/Shanghai
  exclude_schemas: [mysql, sys, information_schema, performance_schema]
ha: { lease_ttl_seconds: 10 }
pipeline: { workers: 1, worker_queue_size: 8192, batch_size: 1 }
targets:
  mysql:
    enabled: true
    addr: "10.0.1.1:3306"
    user: w
    password: x
    charset: utf8mb4
    max_open_conns: 5
    tx_chunk_rows: 500
    target_database: ` + targetDB + `
rules:
` + rulesYAML + `
observability:
  metrics_addr: ":9595"
  log: { level: info }
`
		return writeBootstrapFile(t, dir, "dotted.yml", raw)
	}

	good := mk(`  - {schema: edu.mall.admin, target_database: edu.mall.merge, sinks: {mysql: true},
     tables: [{table: admin_user, order_by_column: id, do_stock: true}]}`,
		"edu.mall.merge")
	if res := Load(context.Background(), good, "", "c1", nil); len(res.Errors) != 0 {
		t.Fatalf("带点库名应通过校验: %v", res.Errors)
	}

	badTable := mk(`  - {schema: edu.mall.admin, sinks: {mysql: true},
     tables: [{table: bad.table, order_by_column: id, do_stock: true}]}`,
		"edu.mall.merge")
	if res := Load(context.Background(), badTable, "", "c1", nil); len(res.Errors) == 0 {
		t.Fatal("表名带点应报错")
	}

	badDB := mk(`  - {schema: edu.mall.admin, sinks: {mysql: true},
     tables: [{table: admin_user, order_by_column: id, do_stock: true}]}`,
		"bad db")
	if res := Load(context.Background(), badDB, "", "c1", nil); len(res.Errors) == 0 {
		t.Fatal("target_database 含空白应报错")
	}
}

// TestHA_ClusterEnabled 集群模式开关三态：缺省开启、显式 true、显式 false。
func TestHA_ClusterEnabled(t *testing.T) {
	if !(HA{}).ClusterEnabled() {
		t.Fatal("缺省（nil）应视为集群模式开启")
	}
	vTrue, vFalse := true, false
	if !(HA{Enabled: &vTrue}).ClusterEnabled() {
		t.Fatal("显式 true 应为集群模式")
	}
	if (HA{Enabled: &vFalse}).ClusterEnabled() {
		t.Fatal("显式 false 应为单节点模式")
	}
}

// writeBootstrapFile 落一份引导文件并返回路径。
func writeBootstrapFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestResolveStartup_FileShape file 形态：整个文件即全量配置，协调面 etcd
// 取 bootstrap.etcd_addrs（位点/选举仍走 etcd，不可省）。
func TestResolveStartup_FileShape(t *testing.T) {
	dir := t.TempDir()
	path := writeBootstrapFile(t, dir, DefaultBootstrapFile, `
cluster: edu-mall
bootstrap:
  etcd_addrs: ["10.0.0.9:2379"]
source: {addr: "10.0.0.1:3306", user: repl, charset: utf8mb4, flavor: mysql, server_id: 1, timezone: Asia/Shanghai}
rules: [{schema: db1, sinks: {mysql: true}}]
`)
	st, err := ResolveStartup(StartupFlags{}, path)
	if err != nil {
		t.Fatalf("file 形态不应报错: %v", err)
	}
	if st.ConfigSource != "file" || st.ConfigFile != path || st.BootstrapFile != path {
		t.Fatalf("file 形态裁决错误: %+v", st)
	}
	if st.Cluster != "edu-mall" {
		t.Fatalf("cluster 应取文件值: %q", st.Cluster)
	}
	if len(st.CoordinationAddrs) != 1 || st.CoordinationAddrs[0] != "10.0.0.9:2379" {
		t.Fatalf("协调面地址错误: %v", st.CoordinationAddrs)
	}
	if len(st.ConfigCenterAddrs) != 0 {
		t.Fatalf("file 形态不应有配置中心: %v", st.ConfigCenterAddrs)
	}
	if st.ConfigKey != DefaultConfigKey("edu-mall") {
		t.Fatalf("key 应为默认: %q", st.ConfigKey)
	}
}

// TestResolveStartup_EtcdShape etcd 形态（生产引导桩）：配置从配置中心拉，
// 协调面缺省回退配置中心同实例；显式 bootstrap.etcd_addrs 时分离两实例。
func TestResolveStartup_EtcdShape(t *testing.T) {
	dir := t.TempDir()
	// 未给协调面地址 → 回退配置中心
	path := writeBootstrapFile(t, dir, "stub.yml", `
cluster: edu-merge-prod
bootstrap:
  config_source: etcd
  config_center:
    etcd_addrs: ["cfg.etcd:2379"]
`)
	st, err := ResolveStartup(StartupFlags{}, path)
	if err != nil {
		t.Fatalf("etcd 形态不应报错: %v", err)
	}
	if st.ConfigSource != "etcd" || st.ConfigFile != "" {
		t.Fatalf("etcd 形态裁决错误: %+v", st)
	}
	if len(st.ConfigCenterAddrs) != 1 || st.ConfigCenterAddrs[0] != "cfg.etcd:2379" {
		t.Fatalf("配置中心地址错误: %v", st.ConfigCenterAddrs)
	}
	if len(st.CoordinationAddrs) != 1 || st.CoordinationAddrs[0] != "cfg.etcd:2379" {
		t.Fatalf("协调面应回退配置中心: %v", st.CoordinationAddrs)
	}

	// 协调面与配置中心分离（位点 etcd 可与配置中心不同实例）
	path = writeBootstrapFile(t, dir, "stub2.yml", `
cluster: edu-merge-prod
bootstrap:
  config_source: etcd
  etcd_addrs: ["coord.etcd:2379"]
  config_center:
    etcd_addrs: ["cfg.etcd:2379"]
`)
	st, err = ResolveStartup(StartupFlags{}, path)
	if err != nil {
		t.Fatalf("双 etcd 形态不应报错: %v", err)
	}
	if st.CoordinationAddrs[0] != "coord.etcd:2379" || st.ConfigCenterAddrs[0] != "cfg.etcd:2379" {
		t.Fatalf("双 etcd 分离失败: coord=%v center=%v", st.CoordinationAddrs, st.ConfigCenterAddrs)
	}

	// etcd 形态缺配置中心 → 报错
	path = writeBootstrapFile(t, dir, "bad.yml", `
cluster: c1
bootstrap: {config_source: etcd}
`)
	if _, err = ResolveStartup(StartupFlags{}, path); err == nil ||
		!strings.Contains(err.Error(), "config_center.etcd_addrs") {
		t.Fatalf("缺配置中心应报错: %v", err)
	}
}

// TestResolveStartup_FlagOverride flag 覆盖引导文件（flag > 文件）。
func TestResolveStartup_FlagOverride(t *testing.T) {
	dir := t.TempDir()
	path := writeBootstrapFile(t, dir, DefaultBootstrapFile, `
cluster: from-file
bootstrap: {etcd_addrs: ["file.etcd:2379"]}
`)
	st, err := ResolveStartup(StartupFlags{
		Cluster: "from-flag", Etcd: "flag.etcd:2379", ConfigKey: "/custom/key",
	}, path)
	if err != nil {
		t.Fatalf("flag 覆盖不应报错: %v", err)
	}
	if st.Cluster != "from-flag" {
		t.Fatalf("cluster 应被 flag 覆盖: %q", st.Cluster)
	}
	if st.CoordinationAddrs[0] != "flag.etcd:2379" {
		t.Fatalf("协调面应被 -etcd 覆盖: %v", st.CoordinationAddrs)
	}
	if st.ConfigKey != "/custom/key" {
		t.Fatalf("key 应被 -config-key 覆盖: %q", st.ConfigKey)
	}

	// file 形态配置（无 bootstrap 段）+ -etcd 覆盖协调面：全量配置读文件本体
	path = writeBootstrapFile(t, dir, "compose.yml", `
cluster: demo
rules: [{schema: db1, sinks: {mysql: true}}]
`)
	st, err = ResolveStartup(StartupFlags{Etcd: "etcd:2379"}, path)
	if err != nil {
		t.Fatalf("compose 形态不应报错: %v", err)
	}
	if st.ConfigSource != "file" || st.CoordinationAddrs[0] != "etcd:2379" {
		t.Fatalf("compose 形态裁决错误: %+v", st)
	}
}

// TestResolveStartup_LegacyFlags 纯 flag 模式：无引导文件时
// -cluster + -etcd 必填，同一 etcd 兼任配置中心与协调面。
func TestResolveStartup_LegacyFlags(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nonexistent.yml")

	// 无文件 + 无 flag → 报错并给出三种启动方式
	_, err := ResolveStartup(StartupFlags{}, missing)
	if err == nil || !strings.Contains(err.Error(), "三选一") {
		t.Fatalf("无文件无 flag 应报错并指引: %v", err)
	}

	// 无文件 + flag 全 → 纯 flag 模式
	st, err := ResolveStartup(StartupFlags{Cluster: "demo", Etcd: "etcd:2379"}, missing)
	if err != nil {
		t.Fatalf("纯 flag 模式不应报错: %v", err)
	}
	if st.ConfigSource != "etcd" || st.BootstrapFile != "" {
		t.Fatalf("纯 flag 模式裁决错误: %+v", st)
	}
	if st.ConfigCenterAddrs[0] != "etcd:2379" || st.CoordinationAddrs[0] != "etcd:2379" {
		t.Fatalf("纯 flag 模式应单 etcd 双角色: center=%v coord=%v", st.ConfigCenterAddrs, st.CoordinationAddrs)
	}

	// 无文件 + flag 缺 → 报错
	if _, err = ResolveStartup(StartupFlags{Cluster: "demo"}, missing); err == nil {
		t.Fatal("缺 -etcd 应报错")
	}
}

// TestResolveStartup_Errors 非法 source、file 形态缺协调面地址、cluster 缺失。
func TestResolveStartup_Errors(t *testing.T) {
	dir := t.TempDir()

	// file 形态无任何 etcd 地址 → 协调面未配置
	path := writeBootstrapFile(t, dir, "noetcd.yml", "cluster: c1\n")
	if _, err := ResolveStartup(StartupFlags{}, path); err == nil ||
		!strings.Contains(err.Error(), "协调面 etcd 未配置") {
		t.Fatalf("file 形态缺协调面应报错: %v", err)
	}

	// source 非法值
	path = writeBootstrapFile(t, dir, "badsrc.yml", "cluster: c1\nbootstrap: {config_source: http}\n")
	if _, err := ResolveStartup(StartupFlags{}, path); err == nil ||
		!strings.Contains(err.Error(), "file|etcd") {
		t.Fatalf("非法 source 应报错: %v", err)
	}

	// cluster 两边都空
	path = writeBootstrapFile(t, dir, "noc.yml", "bootstrap: {etcd_addrs: [\"e:2379\"]}\n")
	if _, err := ResolveStartup(StartupFlags{}, path); err == nil ||
		!strings.Contains(err.Error(), "cluster") {
		t.Fatalf("cluster 缺失应报错: %v", err)
	}
}

// TestLoadBootstrap_EnvPlaceholder 引导文件支持 ${ENV} 占位（与全量配置同链）。
func TestLoadBootstrap_EnvPlaceholder(t *testing.T) {
	t.Setenv("GOCDC_TEST_ETCD", "env.etcd:2379")
	dir := t.TempDir()
	path := writeBootstrapFile(t, dir, DefaultBootstrapFile, `
cluster: c1
bootstrap:
  etcd_addrs: ["${GOCDC_TEST_ETCD}"]
`)
	st, err := ResolveStartup(StartupFlags{}, path)
	if err != nil {
		t.Fatalf("${ENV} 引导不应报错: %v", err)
	}
	if st.CoordinationAddrs[0] != "env.etcd:2379" {
		t.Fatalf("${ENV} 未解析: %v", st.CoordinationAddrs)
	}

	// 缺失变量 → 报错
	path = writeBootstrapFile(t, dir, "missing.yml", `
cluster: c1
bootstrap: {etcd_addrs: ["${GOCDC_TEST_MISSING_VAR}"]}
`)
	if _, err = ResolveStartup(StartupFlags{}, path); err == nil ||
		!strings.Contains(err.Error(), "GOCDC_TEST_MISSING_VAR") {
		t.Fatalf("缺失 ${ENV} 应报错: %v", err)
	}
}
