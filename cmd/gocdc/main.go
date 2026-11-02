// gocdc 入口：MySQL binlog CDC 服务（配置中心/选举/位点均基于 etcd）。
//
// 启动协议（docs/arch/03-etcd-ha-config.md §3.1，零参数优先）：
//
//	gocdc                          # 零参数：读工作目录 config.local.yml（GoLand 调试形态）
//	gocdc -config-file <yml>       # 指定引导/配置文件
//	gocdc -cluster <n> -etcd <addrs> [-config-key <path>] [-config-file <yml>]   # 纯 flag 模式
//
// 两种 etcd 角色可分离：配置中心（bootstrap.config_center，一个 etcd 按集群
// 前缀隔离多环境）与协调面（位点/选举，bootstrap.etcd_addrs）。
//
// 文件分工：main.go（flag/编排）、bootstrap.go（启动裁决/配置/日志/指标/协调面/
// 源库检查）、components.go（组件装配）、lifecycle.go（选举驱动的管线生命周期）、
// state.go（进程级运行状态枢纽）、admin.go（admin API server）、verify_tables.go
// （目标表核对）、source.go/identity.go（源库与节点身份辅助）。
//
// 无子命令——全部运维操作走 admin Web（/api/v1）。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	electorf "gocdc/internal/elector"
	"gocdc/internal/logging"
)

// 版本信息由 CI 注入：-ldflags "-X main.buildVersion=v1.0.0 -X main.buildCommit=xxxx"。
var (
	buildVersion = "dev"
	buildCommit  = "unknown"
)

// options 汇总命令行参数。
//
// flag 全部可选（仅作引导文件之上的覆盖，config.ResolveStartup 裁决）：
// 零参数启动 = 读工作目录 config.local.yml（GoLand 调试形态）；
// 显式 flag 形态（-cluster + -etcd [+ -config-file]）供 docker-compose/CI 直接启动。
type options struct {
	cluster     string
	etcdAddrs   string
	configKey   string // 覆盖配置 key 路径，默认 {prefix}/config
	configFile  string // 引导/配置文件路径覆盖（默认工作目录 config.local.yml）
	showVersion bool
}

func parseFlags(args []string) (*options, error) {
	opts := &options{}
	fs := flag.NewFlagSet("gocdc", flag.ContinueOnError)
	fs.StringVar(&opts.cluster, "cluster", "", "集群名覆盖（缺省取引导文件的 cluster；决定 etcd key 前缀 /gocdc/{cluster}）")
	fs.StringVar(&opts.etcdAddrs, "etcd", "", "协调面 etcd 覆盖，逗号分隔（位点/选举；缺省取引导文件；无引导文件时兼作配置中心）")
	fs.StringVar(&opts.configKey, "config-key", "", "配置 key 路径覆盖（默认 /gocdc/{cluster}/config）")
	fs.StringVar(&opts.configFile, "config-file", "", "引导/配置文件路径覆盖（默认工作目录 config.local.yml）")
	fs.BoolVar(&opts.showVersion, "version", false, "打印版本信息后退出")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return opts, nil
}

func main() {
	opts, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "gocdc:", err)
		os.Exit(2)
	}
	if opts.showVersion {
		fmt.Printf("gocdc %s (%s, %s/%s)\n", buildVersion, buildCommit, runtime.GOOS, runtime.GOARCH)
		return
	}
	if err := run(opts); err != nil {
		fmt.Fprintln(os.Stderr, "gocdc: fatal:", err)
		os.Exit(1)
	}
}

// run 装配并运行服务，阻塞至收到退出信号。
//
// 初始化顺序（多源 PRD 05 §2）：配置加载+严格校验 → 日志 → 指标 → 协调面
// （cluster 级）→ 逐源装配监督（D5：源级失败=failed+重试，不再进程退出）
// → admin API → 选举循环。进程级 fail-fast 保留：etcd/配置/端口等公共依赖
// 失败仍拒绝启动。
// 优雅停机顺序（03 §4.2-4 + PRD 05 §7）：停拉取 → 排空队列 → 刷 sink →
// 保存位点 → Resign（全部源并行 drain）。
//
// 各步返回清理函数，按注册顺序 defer（LIFO = 装配逆序关闭）：
// web 关闭 → 逐源 sink 关闭 → 逐源源库关闭 → 节点注销+session 关闭 →
// 指标关闭 → 日志 Sync → etcd 连接关闭。
func run(opts *options) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ⓪①② 启动裁决 + 配置加载（flag > 引导文件 config.local.yml >
	// 报错；config_source=etcd 时先连配置中心——与协调面可不同实例，file 形态
	// 零 etcd 依赖读文件本体）
	st, cfg, cfgKey, closeCfg, err := resolveStartupAndLoadConfig(ctx, opts)
	if err != nil {
		return err
	}
	defer closeCfg()

	// ③ 日志（配置成功后才可按配置建 logger；之前的错误走 stderr）
	if err := setupLogging(cfg); err != nil {
		return err
	}
	defer logging.Sync()
	logging.L().Info("配置加载完成",
		logging.Event(logging.EventConfigLoaded),
		zap.String("cluster", cfg.Cluster),
		zap.Int("sources", len(cfg.Sources)),
		zap.String("source_ids", strings.Join(cfg.SourceIDs(), ",")),
		zap.String("source", configSourceLabel(st)),
	)

	// ④ 指标与健康端点（/metrics /healthz /readyz，11 §2）
	met, closeMetrics, err := startMetricsServer(cfg, stop)
	if err != nil {
		return err
	}
	defer closeMetrics()

	// ⑤ 协调面 etcd（选举/注册——可与配置中心不同实例）+ lease session
	// + 节点注册（cluster 级；源级 store/markers 在 newApp 的源表里逐源构造）
	cli, kv, closeEtcd, err := connectEtcdAddrs(st.CoordinationAddrs)
	if err != nil {
		return err
	}
	defer closeEtcd()

	coord, closeCoord, err := setupCoordination(ctx, st.Cluster, cfg, kv, cli)
	if err != nil {
		return err
	}
	defer closeCoord()

	// ⑤b 键空间 v1→v2 一次性迁移（PRD 04 §4：幂等 CAS + 完成标记 + 审计；
	// 失败仅告警放行——新代码只读新键，旧键残留无副作用，下次启动重跑）
	if moved, merr := migrateV1Keys(ctx, kv, coord.prefix, coord.nodeID); merr != nil {
		logging.L().Warn("etcd 键迁移未完成（下次启动幂等重试）", zap.Error(merr))
	} else if moved > 0 {
		logging.L().Info("etcd 键迁移完成", zap.Int("moved", moved))
	}

	// ⑥ 逐源装配监督（D5 失败隔离）：srcDB → checkSource → 组件装配+目标表
	// 核对——任一步失败该源 failed+退避重试，其余源不受影响；就绪后与 leader
	// 任期握手（在任则直接启动，PRD 05 §2）
	rt := newApp(opts, cfg, ctx, cli, kv, coord.prefix, coord.sess, coord.reg, coord.nodeID, met)
	for _, src := range rt.sources.all() {
		go rt.superviseSource(ctx, src)
	}
	go rt.metricsLoop(ctx) // gocdc_sources / gocdc_source_up 聚合指标（PRD 07 §1）
	defer func() {
		for _, src := range rt.sources.all() {
			rt.closeSourceComponents(src)
		}
	}()

	// ⑦ admin API（依赖注入；单源视图过渡形态——多源路由随 PRD P3 落地）
	webDeps := buildAdminDeps(st, cfg, cfgKey, coord, rt)

	var runErr error
	if cfg.HA.ClusterEnabled() {
		el := electorf.New(cli, electorf.Options{
			Prefix: coord.prefix, NodeID: coord.nodeID, AdvertiseAddr: cfg.Node.AdvertiseAddr,
			LeaseTTL:            int64(cfg.HA.LeaseTTLSeconds),
			LeaderCheckInterval: time.Duration(cfg.HA.LeaderCheckIntervalMS) * time.Millisecond,
			Log:                 logging.L(),
			OnElected:           rt.onElected, OnLost: rt.onLost,
		})
		webDeps.Elector = el
		webDeps.Resign = func() { el.RequestResign() }
		if closeWeb := startAdminServer(cfg, webDeps); closeWeb != nil {
			defer closeWeb()
		}
		// ⑧ 主生命周期 = 选举循环（ctx 取消 → Resign → OnLost 停全部源）
		runErr = el.Run(ctx)
	} else {
		// 单节点：无 elector（/status role=single、resign 无效），直接 leader 启动
		if closeWeb := startAdminServer(cfg, webDeps); closeWeb != nil {
			defer closeWeb()
		}
		runErr = rt.runSingleLeader(ctx)
	}
	rt.onLost("shutdown")
	rt.waitPipelineExit() // 等待全部源完全退出（位点终值落盘）
	return runErr
}
