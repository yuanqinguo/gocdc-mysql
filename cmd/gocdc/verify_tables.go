// 启动期目标表结构核对（05 §5.1.4 / 10 §3 fail-fast）：
// 规则覆盖的 mysql-sink 表逐张核对，错配置在启动时报全部错误清单，而不是
// 首条事件运行期炸成 PAUSED。附带预热全部规则表结构（schema.Preload 语义）。
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"

	"gocdc/internal/config"
	"gocdc/internal/logging"
	"gocdc/internal/pipeline"
	"gocdc/internal/schema"
	mysqlsink "gocdc/internal/sink/mysql"
)

// verifyTargetTables 规则覆盖的 mysql-sink 表逐张核对（列集、源/目标唯一索引集合、
// 寻址键唯一覆盖、exclude/mask 撞唯一键）；核对清单来自组形态白名单（表清单
// 编译期静态可知，见下方遍历）。srcDB 形参当前未消费，保留签名以免牵动调用方。
func verifyTargetTables(ctx context.Context, srcDB *sql.DB, sm *schema.Manager,
	rules *pipeline.CompiledRules, cfg *config.Config, msink *mysqlsink.Sink) error {

	type schemaTable struct{ schema, table string }
	var tables []schemaTable
	seen := map[string]bool{}
	add := func(s, t string) {
		k := strings.ToLower(s + "." + t)
		if !seen[k] {
			seen[k] = true
			tables = append(tables, schemaTable{s, t})
		}
	}
	for _, r := range cfg.Rules {
		// 组形态白名单：核对清单编译期静态可知，无需查 information_schema 展开
		for _, t := range r.Tables {
			if rules.Match(r.Schema, t.Table) != nil {
				add(r.Schema, t.Table)
			}
		}
	}

	var errs, warns []string
	checked := 0
	for _, t := range tables {
		rule := rules.Match(t.schema, t.table)
		if rule == nil || !rule.Sinks.MySQL {
			continue // 非 mysql sink 表无目标表可核对
		}
		meta, err := sm.Get(ctx, t.schema, t.table)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s.%s 源结构加载失败: %v", t.schema, t.table, err))
			continue
		}
		maskCols := map[string]bool{}
		for c := range rule.MaskColumns {
			maskCols[c] = true
		}
		// 与 sinks.go targetDB 闭包同一解析序（表级 > 全局 > 同名）：
		// 缺同名回退会让默认配置启动即「全表不存在」误报
		targetDB := rule.TargetDatabase
		if targetDB == "" {
			targetDB = cfg.Targets.MySQL.TargetDatabase
		}
		if targetDB == "" {
			targetDB = t.schema
		}
		res := mysqlsink.VerifyTargetTable(ctx, msink.DB(), targetDB, meta,
			rule.ExcludeColumns, maskCols, rule.OrderByColumn)
		errs = append(errs, res.Errors...)
		warns = append(warns, res.Warnings...)
		checked++
	}
	for _, w := range warns {
		fmt.Fprintf(os.Stderr, "目标表告警: %s\n", w)
	}
	if len(errs) > 0 {
		for i, e := range errs {
			fmt.Fprintf(os.Stderr, "目标表核对错误 %d/%d: %s\n", i+1, len(errs), e)
		}
		return fmt.Errorf("目标表结构核对未通过，共 %d 处（05 §5.1.4 fail-fast）", len(errs))
	}
	logging.L().Info("目标表结构核对通过", zap.Int("tables", checked))
	return nil
}
