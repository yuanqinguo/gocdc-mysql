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
	"time"

	"go.uber.org/zap"

	"gocdc/internal/config"
	"gocdc/internal/logging"
	"gocdc/internal/pipeline"
	"gocdc/internal/schema"
	mysqlsink "gocdc/internal/sink/mysql"
)

// verifyTargetTables 规则覆盖的 mysql-sink 表逐张核对（列集、源/目标唯一索引集合、
// 寻址键唯一覆盖、exclude/mask 撞唯一键）；核对清单来自组形态白名单（表清单
// 编译期静态可知，见下方遍历）。srcDB 供启动对账建表消费（目标缺表且
// ddl_sync=true 时按源结构自动建表，2026-11-11）。
// 多源：逐源执行（PRD 05 §1，失败只停该源）。
//
// pendingDDL（"db.table" 小写集合）：有挂起审批记录的表——源已变更、目标等
// 审批是设计内中间态，其核对错误降级为告警（管线重放会停回同一审批，approve
// 后自动对齐；fail-fast 会让挂起等审批期间的重启永远起不来，2026-11-10 修复）。
func verifyTargetTables(ctx context.Context, srcDB *sql.DB, sm *schema.Manager,
	rules *pipeline.CompiledRules, spec *config.SourceSpec, msink *mysqlsink.Sink,
	pendingDDL map[string]bool) error {

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
	for _, r := range spec.Rules {
		// 组形态白名单：核对清单编译期静态可知，无需查 information_schema 展开
		for _, t := range r.Tables {
			if rules.Match(r.Schema, t.Table) != nil {
				add(r.Schema, t.Table)
			}
		}
	}

	var errs, warns []string
	checked := 0
	autoCreated := 0
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
		// 缺同名回退会让默认配置启动即「全表不存在」误报（ddl_sync=true 时
		// 同名回退已在加载校验期拒绝——收紧见 validate.go）
		targetDB := rule.TargetDatabase
		if targetDB == "" {
			targetDB = spec.Targets.MySQL.TargetDatabase
		}
		if targetDB == "" {
			targetDB = t.schema
		}

		// ── 启动对账建表（2026-11-11，08 §2 CREATE 白名单的启动期形态）：
		// 目标缺表且 ddl_sync=true 时按源结构自动建表——新表的 CREATE 事件在
		// 进白名单前已被过滤，运行期 DDL 链路永远等不到它，必须在此补建。
		// 边界：ignore 策略=目标结构另有管理人（维持 fail-fast）；挂起审批表
		// 不动（重放停回同一审批，approve 全量对账含建表）；库级不自动建
		// （库须预建，字符集归运维）。建表后照常走 VerifyTargetTable 全量
		// 核对（列/主键/唯一键与源对齐由生成器保证，此处作二次防线）。
		pkey := strings.ToLower(t.schema + "." + t.table)
		if spec.Targets.MySQL.DDLSync && rule.DDLPolicy != "ignore" && !pendingDDL[pkey] {
			exists, terr := mysqlsink.TableExists(ctx, msink.DB(), targetDB, t.table)
			if terr != nil {
				errs = append(errs, fmt.Sprintf("%s.%s 目标存在性核对失败: %v", t.schema, t.table, terr))
				continue
			}
			if !exists {
				created, cerr := ensureTargetTable(ctx, srcDB, msink.DB(), targetDB, t.schema, t.table)
				if cerr != nil {
					errs = append(errs, cerr.Error())
					continue
				}
				if created {
					autoCreated++
					logging.L().Info("启动对账建表完成（目标缺表，按源结构自动创建）",
						zap.String("source", spec.ID),
						zap.String("table", t.schema+"."+t.table),
						zap.String("target_db", targetDB))
				}
			}
		}

		res := mysqlsink.VerifyTargetTable(ctx, msink.DB(), targetDB, meta,
			rule.ExcludeColumns, maskCols, rule.OrderByColumn)
		if pendingDDL[strings.ToLower(t.schema+"."+t.table)] && len(res.Errors) > 0 {
			// 挂起审批中的表：结构差异为设计内中间态，降级告警（重放停回审批）
			for _, e := range res.Errors {
				warns = append(warns, e+"（该表有 pending DDL 审批——差异属预期中间态，审批后自动对齐）")
			}
		} else {
			errs = append(errs, res.Errors...)
		}
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
	logging.L().Info("目标表结构核对通过", zap.Int("tables", checked), zap.Int("auto_created", autoCreated))
	return nil
}

// ensureTargetTable 目标缺表时按源结构建表（启动对账；diffDefs 的 CREATE
// 白名单语句含列序/主键/唯一键/注释，IF NOT EXISTS 幂等）。表已存在返回
// (false, nil)——装配监督退避重试场景天然幂等。
func ensureTargetTable(ctx context.Context, srcDB, tgtDB *sql.DB, targetDB, srcSchema, table string) (bool, error) {
	res, err := mysqlsink.DiffTargetTable(ctx, srcDB, tgtDB, srcSchema, targetDB, table)
	if err != nil {
		return false, fmt.Errorf("%s.%s 启动对账建表失败（结构 diff）: %w", srcSchema, table, err)
	}
	if !res.CreateTable {
		return false, nil // 已存在（重试/并发窗口），无需建
	}
	if err := mysqlsink.ApplyTargetDDL(ctx, tgtDB, res.AllStmts(), 60*time.Second); err != nil {
		return false, fmt.Errorf("%s.%s 启动对账建表失败: %w", srcSchema, table, err)
	}
	return true, nil
}
