// 启动引导：进程启动所需的三件事——集群名、配置来源、协调面 etcd 地址——
// 默认全部来自工作目录的引导文件（config.local.yml），命令行 flag 仅作覆盖
// （flag > 引导文件 > 报错）。GoLand 调试零参数直接 Run；docker/k8s 沿用 flag。
//
// 两个 etcd 角色可分离（位点 etcd 与配置中心不必同实例）：
//   - 配置中心（bootstrap.config_center）：全量配置的存放地，一个 etcd 按集群
//     前缀 /gocdc/{cluster}/config 隔离多环境；
//   - 协调面（bootstrap.etcd_addrs）：位点/选举/DDL 门/审计，可与配置中心
//     不同实例；缺省回退配置中心地址（同实例部署零额外配置）。
//
// 引导文件两种形态（同一文件，config_source 区分）：
//   - file 形态（默认，开发）：整个文件即全量配置，
//     只需追加 bootstrap.etcd_addrs；
//   - etcd 形态（生产桩）：文件只有 cluster + bootstrap 段，全量配置从配置
//     中心拉取。
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// DefaultBootstrapFile 零参数启动时在工作目录寻找的引导文件名
// （匹配 .gitignore 的 *.local.yml——本地调试文件不入库）。
const DefaultBootstrapFile = "config.local.yml"

// Bootstrap 引导文件中的 bootstrap 段。cluster 复用全量配置的顶层 cluster 字段。
type Bootstrap struct {
	// ConfigSource 配置来源：file（默认）| etcd。
	// 空 = 自动推断：有 config_center.etcd_addrs → etcd，否则 file。
	ConfigSource string `json:"config_source"`
	// EtcdAddrs 协调面 etcd（位点/选举/DDL 门/节点注册/审计）。
	// 空 = 回退 config_center.etcd_addrs（同实例部署）。
	EtcdAddrs []string `json:"etcd_addrs"`
	// ConfigCenter 配置中心（config_source=etcd 时必填）。
	ConfigCenter ConfigCenterOptions `json:"config_center"`
}

// ConfigCenterOptions 配置中心连接信息。
type ConfigCenterOptions struct {
	EtcdAddrs []string `json:"etcd_addrs"` // 配置中心 etcd endpoints
	Key       string   `json:"key"`        // 配置 key；空 = DefaultConfigCenterKey（/config/edu.mall.gocdc/system）
}

// StartupFlags 命令行 flag（全部可选，仅作引导文件/env 之上的覆盖）。
type StartupFlags struct {
	Cluster    string // -cluster
	Etcd       string // -etcd，逗号分隔
	ConfigKey  string // -config-key
	ConfigFile string // -config-file
}

// Startup 裁决后的生效启动参数（ResolveStartup 产物）。
type Startup struct {
	Cluster           string   // 集群名（flag > 引导文件 > gocdc-mysql；决定协调面前缀 /gocdc/{cluster}）
	ConfigSource      string   // "file" | "etcd"
	ConfigFile        string   // file 形态：全量配置文件路径（=引导文件本身）
	BootstrapFile     string   // 实际使用的引导文件路径（"" = 无文件、纯 flag 模式）
	ConfigCenterAddrs []string // 配置中心 etcd（etcd 形态才有）
	ConfigKey         string   // 配置 key（绝对路径，DefaultConfigCenterKey 或覆盖值）
	CoordinationAddrs []string // 协调面 etcd endpoints
}

// ResolveStartup 启动裁决：合并 flag、环境变量与引导文件，产出生效启动参数。
// 优先级：flag > 环境变量（仅 etcd：ETCD_HOSTS）> 引导文件 > 终极默认 > 报错。
// 集群名终极默认 gocdc-mysql；配置键终极默认 DefaultConfigCenterKey（公司约定
// 固定键，多集群共享配置中心时各集群须显式给 key）。纯 flag 模式（无引导文件）：
// -etcd（或 ETCD_HOSTS）必填，同一 etcd 兼任配置中心与协调面。
func ResolveStartup(fl StartupFlags, defaultFile string) (Startup, error) {
	st := Startup{}
	etcdFlag := orDefaultStr(fl.Etcd, os.Getenv("ETCD_HOSTS")) // env 层：仅 etcd 有

	// ① 定位引导文件：-config-file 显式指定 > 工作目录默认文件（存在才用）
	path := fl.ConfigFile
	if path == "" {
		path = defaultFile
		if _, err := os.Stat(path); err != nil {
			path = "" // 默认文件不存在不算错——回落纯 flag 模式
		}
	}

	if path == "" {
		// ② 纯 flag/env 模式（-etcd 或 ETCD_HOSTS 必填；单 etcd 双角色）
		if etcdFlag == "" {
			return st, fmt.Errorf(
				"未找到引导文件 %s 且 -etcd（或环境变量 ETCD_HOSTS）未给。启动方式三选一：\n"+
					"  ① 调试（推荐）：工作目录放 %s，零参数启动（见 internal/config/README.md §2.3）\n"+
					"  ② 生产：引导桩文件（config_source: etcd），配置从配置中心拉取\n"+
					"  ③ 公司约定：ETCD_HOSTS=<addrs> gocdc（cluster=gocdc-mysql，配置读 %s）",
				defaultFile, defaultFile, DefaultConfigCenterKey)
		}
		addrs := strings.Split(etcdFlag, ",")
		st.Cluster = orDefaultStr(fl.Cluster, DefaultClusterName)
		st.ConfigSource = "etcd"
		st.ConfigCenterAddrs = addrs // 同一 etcd 兼任配置中心
		st.ConfigKey = orDefaultKey(fl.ConfigKey)
		st.CoordinationAddrs = addrs
		return st, nil
	}

	// ③ 引导文件模式
	bs, fileCluster, err := loadBootstrap(path)
	if err != nil {
		return st, err
	}
	st.BootstrapFile = path
	st.Cluster = orDefaultStr(fl.Cluster, orDefaultStr(fileCluster, DefaultClusterName))

	source := bs.ConfigSource
	if source == "" {
		if len(bs.ConfigCenter.EtcdAddrs) > 0 {
			source = "etcd"
		} else {
			source = "file"
		}
	}
	st.ConfigSource = source

	switch source {
	case "file":
		// file 形态：引导文件本身即全量配置
		st.ConfigFile = path
		st.ConfigKey = orDefaultKey(fl.ConfigKey) // file 源不读配置中心，key 仅展示用
		st.CoordinationAddrs = coordinationAddrs(etcdFlag, bs)
		if len(st.CoordinationAddrs) == 0 {
			return st, fmt.Errorf("%s: 协调面 etcd 未配置（bootstrap.etcd_addrs、-etcd 或 ETCD_HOSTS 至少一个；"+
				"位点/选举仍依赖 etcd，-config-file 只替代配置来源）", path)
		}
	case "etcd":
		// etcd 形态：引导桩，全量配置在配置中心
		if len(bs.ConfigCenter.EtcdAddrs) == 0 {
			return st, fmt.Errorf("%s: config_source=etcd 但 bootstrap.config_center.etcd_addrs 为空", path)
		}
		st.ConfigCenterAddrs = bs.ConfigCenter.EtcdAddrs
		st.ConfigKey = orDefaultKey(orDefaultStr(fl.ConfigKey, bs.ConfigCenter.Key))
		st.CoordinationAddrs = coordinationAddrs(etcdFlag, bs) // 空 = 回退配置中心同实例
		if len(st.CoordinationAddrs) == 0 {
			st.CoordinationAddrs = st.ConfigCenterAddrs
		}
	default:
		return st, fmt.Errorf("%s: bootstrap.config_source=%q 须为 file|etcd", path, bs.ConfigSource)
	}
	return st, nil
}

// loadBootstrap 从引导文件解析 bootstrap 段与顶层 cluster（复用全量配置的
// 解析链：YAML + ${ENV} 占位）。
func loadBootstrap(path string) (Bootstrap, string, error) {
	var bs Bootstrap
	raw, err := os.ReadFile(path)
	if err != nil {
		return bs, "", fmt.Errorf("读取引导文件 %s: %w", path, err)
	}
	tree, perr := parseYAML(raw)
	if perr != nil {
		return bs, "", perr
	}
	resolved, envErrs := ResolveEnv(tree)
	if len(envErrs) > 0 {
		return bs, "", envErrs[0]
	}
	buf, err := json.Marshal(resolved)
	if err != nil {
		return bs, "", fmt.Errorf("引导文件重新编码失败: %w", err)
	}
	var doc struct {
		Cluster   string    `json:"cluster"`
		Bootstrap Bootstrap `json:"bootstrap"`
	}
	if err := json.Unmarshal(buf, &doc); err != nil {
		return bs, "", fmt.Errorf("引导文件结构解析失败: %w", err)
	}
	return doc.Bootstrap, doc.Cluster, nil
}

// coordinationAddrs 协调面地址裁决：flag -etcd > 环境变量 ETCD_HOSTS >
// bootstrap.etcd_addrs > 空（回退由调用方定）。
func coordinationAddrs(flagEtcd string, bs Bootstrap) []string {
	if flagEtcd != "" {
		return strings.Split(flagEtcd, ",")
	}
	return bs.EtcdAddrs
}

func orDefaultStr(v, def string) string {
	if v != "" {
		return v
	}
	return def
}

// orDefaultKey 配置键裁决：flag/引导桩 key > 终极默认（公司约定固定键，
// DefaultConfigCenterKey——集群名不再参与配置键派生）。
func orDefaultKey(v string) string {
	if v != "" {
		return v
	}
	return DefaultConfigCenterKey
}
