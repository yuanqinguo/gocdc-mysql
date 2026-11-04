// 规则活体预检（api.Deps.Preflight 装配闭包）：对本次变更表（diff 出的
// 新增/修改）用独立短连接做启动核对的活体版——源表存在+寻址键齐备、目标表
// 预建且列集/唯一键一致。不依赖装配态（装配失败正是要改配置的场景），不
// 走运行中管线连接。errors 阻断保存，warnings 放行展示。
//
// 范围口径：只核对本次新增/修改表——存量表上次启动核对已把关；全表核对既
// 慢又会把历史遗留告警误报成本次保存失败。移除表无需核对（目标数据保留
// 不动、stock 死标记由 gcLoop 清理——语义以 warning 形式带出）。
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"gocdc/internal/api"
	"gocdc/internal/config"
	"gocdc/internal/model"
	"gocdc/internal/schema"
	mysqlsink "gocdc/internal/sink/mysql"
)

// preflightRules 活体预检入口（PUT /sources/:sid/rules 调用）。
func (a *app) preflightRules(ctx context.Context, sid string, oldRules, newRules []config.Rule) api.PreflightReport {
	src := a.sources.get(sid)
	if src == nil {
		return api.PreflightReport{Errors: []string{"源 " + sid + " 不在运行配置中（重启后重试）"}}
	}
	spec := src.spec

	added, removed, modified := config.DiffRules(oldRules, newRules)
	report := api.PreflightReport{}
	for _, r := range removed {
		report.Warnings = append(report.Warnings, fmt.Sprintf(
			"移除 %s.%s：目标端数据保留不动，stock 死标记由 GC 清理", r.Schema, r.Table))
	}

	var scope []config.TableRef
	scope = append(scope, added...)
	scope = append(scope, modified...)
	if len(scope) == 0 {
		return report
	}
	if !spec.Targets.MySQL.Enabled {
		report.Warnings = append(report.Warnings, "该源未启用 targets.mysql，跳过目标核对（MQ 下游无目标表结构可核对）")
		return report
	}

	tctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// 独立短连接（源+目标）：预检不依赖装配态，一次性核对后即关闭
	srcDB, err := sql.Open("mysql", sourceDSN(spec.Source))
	if err == nil {
		err = srcDB.PingContext(tctx)
	}
	if err != nil {
		report.Errors = append(report.Errors, "源库不可达，无法预检: "+err.Error())
		return report
	}
	defer srcDB.Close()
	loader := schema.NewMySQLLoader(srcDB)

	tgtDB, err := sql.Open("mysql", targetDSN(spec.Targets.MySQL))
	if err == nil {
		err = tgtDB.PingContext(tctx)
	}
	if err != nil {
		report.Errors = append(report.Errors, "目标库不可达，无法预检: "+err.Error())
		return report
	}
	defer tgtDB.Close()

	for _, ref := range scope {
		group, tb, ok := findTableRule(newRules, ref.Schema, ref.Table)
		if !ok || group.Sinks == nil {
			continue // 组/表在提交内容中不存在（diff 与提交不一致，防御）
		}
		if !group.Sinks.MySQL.Enabled {
			if oldGroup, ok := findGroup(oldRules, ref.Schema); ok && oldGroup.Sinks != nil && oldGroup.Sinks.MySQL.Enabled {
				report.Warnings = append(report.Warnings, fmt.Sprintf(
					"%s.%s：将停止写入 MySQL 目标（sink 变更；历史数据保留不动）", ref.Schema, ref.Table))
			}
			continue
		}

		meta, err := loader.Load(tctx, ref.Schema, ref.Table)
		if err != nil {
			if errors.Is(err, schema.ErrTableNotFound) {
				report.Errors = append(report.Errors, fmt.Sprintf(
					"源表 %s.%s 不存在（先在源库建表再接入）", ref.Schema, ref.Table))
			} else {
				report.Errors = append(report.Errors, fmt.Sprintf(
					"源表 %s.%s 结构加载失败: %v", ref.Schema, ref.Table, err))
			}
			continue
		}
		// order_by_column 必须真实存在（快照游标/伪主键寻址的前提；
		// VerifyTargetTable 只查覆盖关系，存在性在此把关）
		if tb.OrderByColumn != "" && metaColumn(meta, tb.OrderByColumn) == nil {
			report.Errors = append(report.Errors, fmt.Sprintf(
				"源表 %s.%s 无列 %s（order_by_column 配置错误）", ref.Schema, ref.Table, tb.OrderByColumn))
			continue
		}

		maskCols := map[string]bool{}
		for c := range tb.MaskColumns {
			maskCols[c] = true
		}
		targetDatabase := targetDBOf(spec, group, &tb, ref.Schema)
		res := mysqlsink.VerifyTargetTable(tctx, tgtDB, targetDatabase, meta,
			tb.ExcludeColumns, maskCols, tb.OrderByColumn)
		report.Checked++
		report.Errors = append(report.Errors, res.Errors...)
		report.Warnings = append(report.Warnings, res.Warnings...)
	}
	return report
}

// findTableRule 在规则集中定位 schema.table（大小写不敏感，与白名单匹配口径一致）。
func findTableRule(rules []config.Rule, schemaName, table string) (*config.Rule, config.TableRule, bool) {
	group, ok := findGroup(rules, schemaName)
	if !ok {
		return nil, config.TableRule{}, false
	}
	for i := range group.Tables {
		if strings.EqualFold(group.Tables[i].Table, table) {
			return group, group.Tables[i], true
		}
	}
	return nil, config.TableRule{}, false
}

// findGroup 按库名定位规则组（大小写不敏感）。
func findGroup(rules []config.Rule, schemaName string) (*config.Rule, bool) {
	for i := range rules {
		if strings.EqualFold(rules[i].Schema, schemaName) {
			return &rules[i], true
		}
	}
	return nil, false
}

// targetDBOf 目标库解析（与 components.targetDB 闭包同一优先级：
// 表级 > 组级 > targets.mysql.target_database > 同名）。
func targetDBOf(spec *config.SourceSpec, group *config.Rule, tb *config.TableRule, schemaName string) string {
	if tb.TargetDatabase != "" {
		return tb.TargetDatabase
	}
	if group.TargetDatabase != "" {
		return group.TargetDatabase
	}
	if spec.Targets.MySQL.TargetDatabase != "" {
		return spec.Targets.MySQL.TargetDatabase
	}
	return schemaName
}

// metaColumn 列存在性查找。
func metaColumn(meta *model.TableMeta, name string) *model.Column {
	for i := range meta.Columns {
		if strings.EqualFold(meta.Columns[i].Name, name) {
			return &meta.Columns[i]
		}
	}
	return nil
}

// targetDSN 目标库 DSN（与 sourceDSN 同形态；预检一次性连接）。
func targetDSN(m config.MySQLOptions) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/?charset=%s", m.User, m.Password, m.Addr, m.Charset)
}
