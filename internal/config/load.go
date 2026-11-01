package config

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"gocdc/internal/etcdx"
)

// LoadResult 加载结果：Config 与全部错误（03 §3.1-3：校验失败打印全部错误后退出，
// 绝不带部分配置运行）。
type LoadResult struct {
	Config *Config
	Errors []error
}

// Load 统一加载入口（03 §3.1 协议）：
//  1. 配置源：configFile 非空 → 本地文件（仅开发/单机）；否则读 etcd configKey
//  2. 解析为配置树：YAML 唯一格式
//  3. ${ENV} 解析（缺失变量收集错误）
//  4. cluster 先后裁决：配置体 cluster 必须等于 flagCluster，不等即报
//     「配置属于别的集群」（flag 是 key 前缀唯一来源，配置字段仅冗余校验）
//  5. 填默认值 → 严格校验，返回全部错误
func Load(ctx context.Context, cfgFile, etcdKey, flagCluster string, kv *etcdx.KV) LoadResult {
	var (
		raw []byte
		err error
	)
	if cfgFile != "" {
		raw, err = os.ReadFile(cfgFile)
		if err != nil {
			return LoadResult{Errors: []error{fmt.Errorf("读取配置文件 %s: %w", cfgFile, err)}}
		}
	} else {
		resp, gerr := kv.Get(ctx, etcdKey)
		if gerr != nil {
			return LoadResult{Errors: []error{fmt.Errorf("读取 etcd 配置 %s: %w", etcdKey, gerr)}}
		}
		if len(resp.Kvs) == 0 {
			return LoadResult{Errors: []error{fmt.Errorf("etcd 配置 %s 不存在（绝不带空/旧配置运行，见 03 §3.1-2）", etcdKey)}}
		}
		raw = resp.Kvs[0].Value
	}

	tree, perr := parseYAML(raw)
	if perr != nil {
		return LoadResult{Errors: []error{perr}}
	}
	resolved, envErrs := ResolveEnv(tree)
	buf, err := json.Marshal(resolved)
	if err != nil {
		return LoadResult{Errors: []error{fmt.Errorf("配置重新编码失败: %w", err)}}
	}
	var cfg Config
	if err := json.Unmarshal(buf, &cfg); err != nil {
		return LoadResult{Errors: []error{fmt.Errorf("配置结构解析失败: %w", err)}}
	}

	errs := append([]error{}, envErrs...)
	ApplyDefaults(&cfg)
	errs = append(errs, Validate(&cfg, flagCluster)...)
	return LoadResult{Config: &cfg, Errors: errs}
}

// parseYAML 把配置正文按 YAML 解析为通用树（唯一支持格式）。空文档（全空白
// 或仅注释）显式报错，交给
// 上层给出精确提示而非一堆校验缺失。
func parseYAML(raw []byte) (any, error) {
	var tree any
	if err := yaml.Unmarshal(raw, &tree); err != nil {
		return nil, fmt.Errorf("配置 YAML 解析失败: %w", err)
	}
	if tree == nil {
		return nil, fmt.Errorf("配置为空（须为非空 YAML 文档）")
	}
	return tree, nil
}

// ApplyDefaults 填充周期类参数的默认值（0 → 默认；结构性/安全性字段不设默认，
// 留给 Validate 强制显式配置）。
func ApplyDefaults(c *Config) {
	set := func(v *int, d int) {
		if *v == 0 {
			*v = d
		}
	}
	set(&c.Source.HeartbeatIntervalMS, 3000)
	set(&c.Source.ReadTimeoutMS, 60000)
	set(&c.HA.LeaderCheckIntervalMS, 2000)
	set(&c.Pipeline.FlushIntervalMS, 200)
	set(&c.Pipeline.PositionSaveIntervalMS, 3000)
	set(&c.Targets.MySQL.MaxIdleConns, 5)
	set(&c.Targets.MySQL.ConnMaxLifetimeMin, 30)
	set(&c.Targets.Kafka.SendTimeoutMS, 5000)
	set(&c.Targets.RocketMQ.SendTimeoutMS, 5000)
	set(&c.Targets.RocketMQ.SendConcurrency, 32)
	if c.Stock.ChunkRows == 0 {
		c.Stock.ChunkRows = 5000
	}
	if c.Stock.Workers == 0 {
		c.Stock.Workers = 4
	}
	if c.Pipeline.OnConvertError == "" {
		c.Pipeline.OnConvertError = "null"
	}
	if c.Pipeline.DecimalAs == "" {
		c.Pipeline.DecimalAs = "string"
	}
	if c.Pipeline.ZeroDatetimeAs == "" {
		c.Pipeline.ZeroDatetimeAs = "null"
	}
}

// DefaultConfigKey 返回 {prefix}/config（prefix=/gocdc/{cluster}）。
func DefaultConfigKey(cluster string) string {
	return "/gocdc/" + cluster + "/config"
}
