// 启动引导（fail-fast，任一步失败立即退出，03 §3.1）：etcd 客户端 → 配置加载
// → 日志 → 指标端点 → 协调面（session/节点注册/位点与标记存储）→ 源库连接 +
// 启动检查。各步返回的清理函数由调用方在 run() 中按注册顺序 defer——LIFO
// 即按装配逆序关闭。
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"

	checkf "gocdc/internal/check"
	"gocdc/internal/config"
	"gocdc/internal/etcdx"
	"gocdc/internal/logging"
	"gocdc/internal/metrics"
)

// connectEtcdAddrs 建立指向给定 endpoints 的 etcd 客户端（连接惰性，首个
// 操作才真正建连；调用方用 KV 包装层的重试吸收瞬时故障）。
func connectEtcdAddrs(addrs []string) (*clientv3.Client, *etcdx.KV, func(), error) {
	c, err := etcdx.New(etcdx.Options{Endpoints: addrs})
	if err != nil {
		return nil, nil, nil, err
	}
	return c, etcdx.NewKV(c), func() { c.Close() }, nil
}

// resolveStartupAndLoadConfig 启动裁决 + 配置加载：flag > 引导文件（工作目录
// config.local.yml）> 报错；
// config_source=etcd 时先连配置中心（与协调面可不同实例），file 形态零 etcd
// 依赖读文件。返回生效启动参数、全量配置、配置 key 与配置中心清理函数
// （file 形态为 no-op）。
func resolveStartupAndLoadConfig(ctx context.Context, opts *options) (*config.Startup, *config.Config, string, func(), error) {
	st, err := config.ResolveStartup(config.StartupFlags{
		Cluster: opts.cluster, Etcd: opts.etcdAddrs,
		ConfigKey: opts.configKey, ConfigFile: opts.configFile,
	}, config.DefaultBootstrapFile)
	if err != nil {
		return nil, nil, "", nil, err
	}

	var cfgKV *etcdx.KV
	closeCfg := func() {}
	if st.ConfigSource == "etcd" {
		// 客户端引用由 etcdx.KV 持有；closeCfg 负责关闭
		_, cfgKV, closeCfg, err = connectEtcdAddrs(st.ConfigCenterAddrs)
		if err != nil {
			return nil, nil, "", nil, fmt.Errorf("配置中心 etcd 连接失败: %w", err)
		}
	}

	cfg, cfgKey, lerr := loadConfig(ctx, &st, cfgKV)
	if lerr != nil {
		closeCfg()
		return nil, nil, "", nil, lerr
	}
	return &st, cfg, cfgKey, closeCfg, nil
}

// loadConfig 配置加载：file 形态读引导文件本身，etcd 形态读配置中心 key
// （03 §3.1-1）。校验失败打印全部错误清单后退出——绝不带部分配置运行。
func loadConfig(ctx context.Context, st *config.Startup, kv *etcdx.KV) (*config.Config, string, error) {
	cfgFile := ""
	if st.ConfigSource == "file" {
		cfgFile = st.ConfigFile
	}
	res := config.Load(ctx, cfgFile, st.ConfigKey, st.Cluster, kv)
	if len(res.Errors) > 0 {
		for i, e := range res.Errors {
			fmt.Fprintf(os.Stderr, "配置错误 %d/%d: %v\n", i+1, len(res.Errors), e)
		}
		return nil, "", fmt.Errorf("配置校验失败，共 %d 处", len(res.Errors))
	}
	return res.Config, st.ConfigKey, nil
}

// configSourceLabel 配置来源标签（启动日志与 admin /status 展示用）。
func configSourceLabel(st *config.Startup) string {
	if st.ConfigSource == "file" {
		return "file:" + st.ConfigFile
	}
	return "etcd:" + st.ConfigKey
}

// setupLogging 日志初始化（配置成功后才可按配置建 logger；之前的错误走 stderr）。
func setupLogging(cfg *config.Config) error {
	_, err := logging.Setup(logging.Config{
		Level: cfg.Observability.Log.Level,
		JSON:  cfg.Observability.Log.JSON,
	})
	return err
}

// startMetricsServer 指标与健康端点（/metrics /healthz /readyz，11 §2）。
// 探针：config_loaded（能走到这里必然已加载）与 pipeline_running（多源口径：
// 任一源增量管线在跑即就绪——失败隔离下个别源 failed 不打红整个进程探针）。
// 端口占用等致命错误 → 调 stop() 触发停机（fail-fast）。返回清理函数。
func startMetricsServer(cfg *config.Config, stop context.CancelFunc) (*metrics.Metrics, func(), error) {
	met := metrics.New()
	mx := metrics.NewServer()
	mx.RegisterProbe("config_loaded", func() bool { return true })
	mx.RegisterProbe("pipeline_running", func() bool { return pipelineRunningSources.Load() > 0 })
	srv := &http.Server{Addr: cfg.Observability.MetricsAddr, Handler: mx.Handler(met)}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logging.L().Error("指标端口监听失败", zap.Error(err))
			stop() // 端口占用等致命错误 → 触发停机（fail-fast）
		}
	}()
	logging.L().Info("指标端点就绪", zap.String("addr", cfg.Observability.MetricsAddr))
	return met, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}

// coordination 协调面：选举 session 与节点注册（cluster 级共享）。
// 位点/全量标记存储按源实例化（键空间 v2：{prefix}/sources/{sid}/…，
// 见 newSourceTable），不再挂在 coordination 上。
type coordination struct {
	prefix string
	kv     *etcdx.KV
	cli    *clientv3.Client
	sess   *concurrency.Session
	reg    *etcdx.Registrar
	nodeID string
}

// setupCoordination etcd 协调面（03 §5）：lease session + 节点注册（role 初始
// leader，选举/失位回调改写）。源级 store 初始无门槛（leaseID=0 写全拒，
// 写门槛全覆盖：follower/无主绝不写位点；SetGate 在源流程启动时注入）。
// 协调面 etcd 地址来自启动裁决（bootstrap.etcd_addrs，可与配置中心不同实例）。
func setupCoordination(ctx context.Context, cluster string, cfg *config.Config,
	kv *etcdx.KV, cli *clientv3.Client) (*coordination, func(), error) {

	prefix := "/gocdc/" + cluster
	sess, err := etcdx.NewSession(ctx, cli, int64(cfg.HA.LeaseTTLSeconds))
	if err != nil {
		return nil, nil, err
	}
	nodeID := cfg.Node.ID
	if nodeID == "" {
		nodeID = deriveNodeID()
	}
	// server_id 缺省派生（多源 PRD 05 §6）：按 node.id:sid 逐源派生——
	// 同进程各源互异（sid 不同）、同源多副本互异（node.id 不同）；显式配置
	// 优先。回填后 lifecycle 的 source 构造（cfg 同指针）自然取到。
	for i := range cfg.Sources {
		spec := &cfg.Sources[i]
		if spec.Source.ServerID == 0 {
			spec.Source.ServerID = deriveServerID(nodeID + ":" + spec.ID)
			logging.L().Info("server_id 缺省，已按 node.id:source_id 派生",
				zap.Uint32("server_id", spec.Source.ServerID),
				zap.String("node_id", nodeID), zap.String("source_id", spec.ID))
		}
	}
	reg := etcdx.NewRegistrar(kv, prefix, etcdx.NodeInfo{
		NodeID: nodeID, AdvertiseAddr: cfg.Node.AdvertiseAddr,
		Version: buildVersion, Role: "leader", // 初始 leader：单节点恒为该值；集群模式由 elector 改写
	})
	if err := reg.Register(ctx, sess); err != nil {
		_ = sess.Close()
		return nil, nil, fmt.Errorf("节点注册失败: %w", err)
	}
	logging.L().Info("节点已注册", zap.String("node_id", nodeID), zap.String("key", reg.Key()))

	c := &coordination{
		prefix: prefix,
		kv:     kv, cli: cli,
		sess:   sess,
		reg:    reg,
		nodeID: nodeID,
	}
	// 关闭按装配逆序：先注销节点，后关 session
	cleanup := func() {
		_ = reg.Deregister(context.Background())
		sess.Close()
	}
	return c, cleanup, nil
}

// openSourceDB 源库连接（schema 加载 / 位点采样 / 全量扫描 / binlog 拉取共用）。
// 每源独立池（多源 PRD 05 §1）。
func openSourceDB(spec *config.SourceSpec) (*sql.DB, error) {
	db, err := sql.Open("mysql", sourceDSN(spec.Source))
	if err != nil {
		return nil, err
	}
	// 池容量 = 常规开销 4 + 快照连接集（stock.workers，ApplyDefaults 后 ≥1）：
	// 快照会话一表占一根连接，块间 schemaChanged/位点采样仍需走池——容量不足
	// 会在全量期间永久阻塞
	db.SetMaxOpenConns(4 + spec.Stock.Workers)
	return db, nil
}

// checkSource 启动检查器（源端要求全项，必须项失败拒绝启动）。
func checkSource(ctx context.Context, spec *config.SourceSpec, srcDB *sql.DB) error {
	if err := srcDB.PingContext(ctx); err != nil {
		return fmt.Errorf("源库不可达: %w", err)
	}
	res := checkf.Source(ctx, srcDB, checkf.Options{
		UseGTID: spec.Source.UseGTID, ServerID: spec.Source.ServerID,
	})
	logging.L().Info("启动检查（源库环境）",
		zap.String("source_id", spec.ID),
		zap.Int("errors", len(res.Errors)), zap.Int("warnings", len(res.Warnings)))
	fmt.Fprintln(os.Stderr, res.Summary())
	if !res.OK() {
		return fmt.Errorf("启动检查未通过（%d 项必须项失败），拒绝启动（fail-fast）", len(res.Errors))
	}
	return nil
}
