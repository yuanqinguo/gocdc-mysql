package config

import (
	"context"
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

// Load 统一加载入口（03 §3.1 协议；多源 PRD 03 §4）：
//  1. 配置源：configFile 非空 → 本地文件（仅开发/单机）；否则读 etcd configKey
//  2. 解析为配置树：YAML 唯一格式
//  3. ${ENV} 解析（缺失变量收集错误）
//  4. 结构反序列化：配置面统一 sources[]（单源=数组一条）；顶层
//     source/targets/rules 为已移除的 v1 形态，命中即报错并给改写指引
//  5. cluster 先后裁决：配置体 cluster 必须等于 flagCluster，不等即报
//     「配置属于别的集群」（flag 是 key 前缀唯一来源，配置字段仅冗余校验）
//  6. 源级覆盖合并 + 填默认值 → 严格校验，返回全部错误
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
	return LoadFromRaw(raw, flagCluster)
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

// rejectV1Form 顶层 source/targets/rules（历史 v1 单源形态）检测：v2 起
// 配置面统一只认 sources[]（2026-11 用户决策：不再自动包装、不做双写裁决，
// 单源即数组一条），命中即报错并给出改写指引——绝不静默忽略（根结构体无
// 这些字段，无此检查会被无声丢弃）。
func rejectV1Form(tree any) []error {
	root, ok := tree.(map[string]any)
	if !ok {
		return nil // 非对象树（如标量）交给反序列化报结构错误
	}
	for _, k := range []string{"source", "targets", "rules"} {
		if _, exists := root[k]; exists {
			return []error{fmt.Errorf(
				"检测到顶层 %s 键（v1 单源形态已移除）：配置统一为 sources[] 数组（单源即数组一条）——请改写为 sources: [{id: ..., source/targets/rules: ...}]，见 internal/config/README.md §4.5", k)}
		}
	}
	return nil
}

// ApplyDefaults 填充周期类参数的默认值（0 → 默认；结构性/安全性字段不设默认，
// 留给 Validate 强制显式配置）。多源（PRD 03 §1.3）：先把源级 pipeline/stock
// 覆盖补丁合并进集群默认（幂等——合并值非零字段覆盖集群默认恒得自身），
// 再逐源填默认——校验与运行时看到的每个源都是已合并的完整值。
func ApplyDefaults(c *Config) {
	set := func(v *int, d int) {
		if *v == 0 {
			*v = d
		}
	}
	set(&c.HA.LeaderCheckIntervalMS, 2000)
	// 集群默认模板自身也填默认（/config 展示完整模板；源级合并以此为基）
	set(&c.Pipeline.FlushIntervalMS, 200)
	set(&c.Pipeline.PositionSaveIntervalMS, 3000)
	if c.Pipeline.OnConvertError == "" {
		c.Pipeline.OnConvertError = "null"
	}
	if c.Pipeline.DecimalAs == "" {
		c.Pipeline.DecimalAs = "string"
	}
	if c.Pipeline.ZeroDatetimeAs == "" {
		c.Pipeline.ZeroDatetimeAs = "null"
	}
	set(&c.Stock.ChunkRows, 5000)
	set(&c.Stock.Workers, 4)
	for i := range c.Sources {
		spec := &c.Sources[i]
		merged := spec.ResolvePipeline(c.Pipeline)
		spec.Pipeline = &merged
		mergedStock := spec.ResolveStock(c.Stock)
		spec.Stock = &mergedStock

		set(&spec.Pipeline.FlushIntervalMS, 200)
		set(&spec.Pipeline.PositionSaveIntervalMS, 3000)
		if spec.Pipeline.OnConvertError == "" {
			spec.Pipeline.OnConvertError = "null"
		}
		if spec.Pipeline.DecimalAs == "" {
			spec.Pipeline.DecimalAs = "string"
		}
		if spec.Pipeline.ZeroDatetimeAs == "" {
			spec.Pipeline.ZeroDatetimeAs = "null"
		}
		set(&spec.Stock.ChunkRows, 5000)
		set(&spec.Stock.Workers, 4)

		set(&spec.Source.HeartbeatIntervalMS, 3000)
		set(&spec.Source.ReadTimeoutMS, 60000)
		set(&spec.Targets.MySQL.MaxIdleConns, 5)
		set(&spec.Targets.MySQL.ConnMaxLifetimeMin, 30)
		set(&spec.Targets.Kafka.SendTimeoutMS, 5000)
		set(&spec.Targets.RocketMQ.SendTimeoutMS, 5000)
		set(&spec.Targets.RocketMQ.SendConcurrency, 32)
	}
}

// DefaultConfigCenterKey 说明见 config.go（/config/{服务完整名}/system）；
// 历史的集群作用域键 /gocdc/{cluster}/config 不再是默认——多集群共享配置中心
// 时各集群以引导桩 config_center.key 或 -config-key 显式区分。
