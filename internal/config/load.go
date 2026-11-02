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

// Load 统一加载入口（03 §3.1 协议；多源 PRD 03 §4）：
//  1. 配置源：configFile 非空 → 本地文件（仅开发/单机）；否则读 etcd configKey
//  2. 解析为配置树：YAML 唯一格式
//  3. ${ENV} 解析（缺失变量收集错误）
//  4. v1→v2 规范化：顶层 source/targets/rules 自动包装为 sources[default]，
//     双写报错——下游只见 v2 结构，v1 不是平行代码路径，只是 N=1 的普通源
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

	tree, perr := parseYAML(raw)
	if perr != nil {
		return LoadResult{Errors: []error{perr}}
	}
	resolved, envErrs := ResolveEnv(tree)
	normalized, normErrs := normalizeTree(resolved)
	buf, err := json.Marshal(normalized)
	if err != nil {
		return LoadResult{Errors: append(envErrs, fmt.Errorf("配置重新编码失败: %w", err))}
	}
	var cfg Config
	if err := json.Unmarshal(buf, &cfg); err != nil {
		return LoadResult{Errors: append(envErrs, fmt.Errorf("配置结构解析失败: %w", err))}
	}

	errs := append([]error{}, envErrs...)
	errs = append(errs, normErrs...)
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

// normalizeTree v1→v2 结构规范化（多源 PRD 03 §4，发生在 ${ENV} 解析之后、
// 反序列化之前——下游校验/装配/API 只见 v2）：
//
//	tree 无 sources 且含顶层 source/targets/rules 任一 → 包装为
//	sources: [{id: "default", ...}]，顶层旧字段移除；
//	tree 同时含 sources 与顶层旧字段 → 双写歧义报错。
//
// 缺失的 v1 子段以空对象补位：后续校验按 [sources/default] 前缀逐项报缺，
// 错误定位与 v1 语义一致。v1 顶层 pipeline/stock 即集群默认，被唯一源继承，
// 值不变。
func normalizeTree(tree any) (any, []error) {
	root, ok := tree.(map[string]any)
	if !ok {
		return tree, nil // 非对象树（如标量）交给反序列化报结构错误
	}
	v1Fields := []string{"source", "targets", "rules"}
	hasV1 := false
	for _, k := range v1Fields {
		if _, exists := root[k]; exists {
			hasV1 = true
		}
	}
	srcs, hasSources := root["sources"]
	if hasSources && srcs != nil {
		if hasV1 {
			return tree, []error{fmt.Errorf(
				"配置同时给出 sources 与顶层 source/targets/rules（不允许双写歧义）：v1 旧字段会自动包装为 sources[default]，请移除旧字段或并入 sources")}
		}
		return tree, nil // 纯 v2：不动
	}
	if !hasV1 {
		return tree, nil // 两边都没有：Sources 为空由校验 V1 报「至少 1 条」
	}
	get := func(k string, empty any) any {
		if v, exists := root[k]; exists && v != nil {
			return v
		}
		return empty // 缺失子段补空值（map/list 按字段类型），校验逐项报缺
	}
	root["sources"] = []any{map[string]any{
		"id":      DefaultSourceID,
		"source":  get("source", map[string]any{}),
		"targets": get("targets", map[string]any{}),
		"rules":   get("rules", []any{}),
	}}
	for _, k := range v1Fields {
		delete(root, k)
	}
	return root, nil
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

// DefaultConfigKey 返回 {prefix}/config（prefix=/gocdc/{cluster}）。
func DefaultConfigKey(cluster string) string {
	return "/gocdc/" + cluster + "/config"
}
