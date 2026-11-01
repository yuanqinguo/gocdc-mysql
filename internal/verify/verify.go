// Package verify 源/目标表抽样对账：
// 全表 COUNT + 按主键随机抽样逐行按列比对。
//
// 唯一入口 = Web `POST /api/v1/verify`（confirm=true；装配层闭包注入进程内
// 源/目标连接）。运维操作不提供 CLI 形态——Docker 部署下 Web 点击即达，
// 无需进节点。
//
// 比对语义：列清单取自源库 information_schema（ORDINAL_POSITION 固定序）；
// 值按驱动字符串形态比对（NULL 标记 <NULL>，与空串可区分）——检测的是
// 「行存在性 + 值一致」，不校验列类型差异。抽样 ORDER BY RAND() 在大表上
// 有全扫代价（运维工具可接受；抽样键须有索引）。样本上限 1000 行防误触发
// 大表扫描。
package verify

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// Options 对账参数。
type Options struct {
	SrcDB    *sql.DB
	TgtDB    *sql.DB
	Schema   string // 源库
	TargetDB string // 目标库（可与源同名）
	Table    string
	PK       string // 抽样/寻址键
	Sample   int    // 抽样行数；<=0=100，>1000 截断
	MaxDiffs int    // 差异明细条数上限；<=0=20
}

// Result 对账结果（Mismatch>0 = 数据差异，非工具错误；error 仅基础设施失败）。
type Result struct {
	SrcCount  int64    `json:"src_count"`
	TgtCount  int64    `json:"tgt_count"`
	Checked   int      `json:"checked"`
	Mismatch  int      `json:"mismatch"`
	Diffs     []string `json:"diffs"`     // 差异明细（MISS/DIFF，最多 MaxDiffs 条）
	Truncated bool     `json:"truncated"` // 明细超上限截断
}

// OK 数据一致。
func (r *Result) OK() bool { return r.Mismatch == 0 }

// CompareTable 执行对账。error 仅基础设施失败（连不上/表不存在）；数据差异
// 体现在 Result.Mismatch（Web 端据此展示而非报 500）。
func CompareTable(ctx context.Context, o *Options) (*Result, error) {
	if o.SrcDB == nil || o.TgtDB == nil {
		return nil, fmt.Errorf("verify: SrcDB/TgtDB 必填")
	}
	if o.Schema == "" || o.Table == "" || o.PK == "" {
		return nil, fmt.Errorf("verify: Schema/Table/PK 必填")
	}
	if o.TargetDB == "" {
		o.TargetDB = o.Schema
	}
	if o.Sample <= 0 {
		o.Sample = 100
	}
	if o.Sample > 1000 {
		o.Sample = 1000
	}
	if o.MaxDiffs <= 0 {
		o.MaxDiffs = 20
	}
	res := &Result{}

	// ── 全表 COUNT（schema 限定名——连接可能无默认库：Web 闭包复用进程级
	// 连接、DSN 不带库）──
	if err := o.SrcDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM "+quoteIdent(o.Schema)+"."+quoteIdent(o.Table)).Scan(&res.SrcCount); err != nil {
		return res, fmt.Errorf("verify: 源表 COUNT: %w", err)
	}
	if err := o.TgtDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM "+quoteIdent(o.TargetDB)+"."+quoteIdent(o.Table)).Scan(&res.TgtCount); err != nil {
		return res, fmt.Errorf("verify: 目标表 COUNT: %w", err)
	}

	// ── 列清单（源库 information_schema，固定序）——按列比对而非 SELECT *
	// 扫单值（多列行扫进单个 dest 必报 sql 错）──
	qCols, err := o.SrcDB.QueryContext(ctx,
		`SELECT COLUMN_NAME FROM information_schema.COLUMNS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`,
		o.Schema, o.Table)
	if err != nil {
		return res, fmt.Errorf("verify: 列清单查询: %w", err)
	}
	var cols []string
	for qCols.Next() {
		var c string
		if err := qCols.Scan(&c); err != nil {
			qCols.Close()
			return res, err
		}
		cols = append(cols, c)
	}
	qCols.Close()
	if err := qCols.Err(); err != nil {
		return res, err
	}
	if len(cols) == 0 {
		return res, fmt.Errorf("verify: 源库 %s.%s 无列信息（表不存在？）", o.Schema, o.Table)
	}

	// ── 抽样主键 ──
	rows, err := o.SrcDB.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s ORDER BY RAND() LIMIT %d",
			quoteIdent(o.PK), quoteIdent(o.Schema)+"."+quoteIdent(o.Table), o.Sample))
	if err != nil {
		return res, fmt.Errorf("verify: 抽样查询: %w", err)
	}
	var ids []any
	for rows.Next() {
		var v any
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return res, err
		}
		ids = append(ids, v)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return res, err
	}

	// ── 逐行按列比对（源/目标各自带库限定名）──
	fetchSrc := rowFetcher(ctx, o.Schema, o, cols)
	fetchTgt := rowFetcher(ctx, o.TargetDB, o, cols)
	for _, id := range ids {
		s1, e1 := fetchSrc(o.SrcDB, id)
		s2, e2 := fetchTgt(o.TgtDB, id)
		switch {
		case e1 != nil || e2 != nil:
			res.Mismatch++
			res.addDiff(o, fmt.Sprintf("MISS pk=%v err=%v|%v", id, e1, e2))
		case s1 != s2:
			res.Mismatch++
			res.addDiff(o, fmt.Sprintf("DIFF pk=%v src=%s dst=%s", id, s1, s2))
		default:
			res.Checked++
		}
	}
	return res, nil
}

// addDiff 追加差异明细（超 MaxDiffs 截断，只保留计数）。
func (r *Result) addDiff(o *Options, line string) {
	if len(r.Diffs) < o.MaxDiffs {
		r.Diffs = append(r.Diffs, line)
	} else {
		r.Truncated = true
	}
}

// rowFetcher 单行取值：NullString 扫描 + 列名 JSON 序列化（NULL 标记
// <NULL>，与空串可区分）。注意 RawBytes 不允许用于 QueryRow 的 Scan
// （database/sql 约束，仅 Rows 迭代可用）——NullString 无复用限制。
// dbName 为库限定名（源库/目标库各构造一份）。
func rowFetcher(ctx context.Context, dbName string, o *Options, cols []string) func(*sql.DB, any) (string, error) {
	colList := make([]string, len(cols))
	for i, c := range cols {
		colList[i] = quoteIdent(c)
	}
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?",
		strings.Join(colList, ","), quoteIdent(dbName)+"."+quoteIdent(o.Table), quoteIdent(o.PK))
	return func(db *sql.DB, id any) (string, error) {
		scan := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range scan {
			ptrs[i] = &scan[i]
		}
		if err := db.QueryRowContext(ctx, q, id).Scan(ptrs...); err != nil {
			return "", err
		}
		out := make(map[string]string, len(cols))
		for i, c := range cols {
			if !scan[i].Valid {
				out[c] = "<NULL>"
			} else {
				out[c] = scan[i].String
			}
		}
		buf, err := json.Marshal(out)
		if err != nil {
			return "", err
		}
		return string(buf), nil
	}
}

// quoteIdent 标识符反引号包裹（内部反引号翻倍转义）——表/列名来自 Web
// 请求体与 information_schema，做基本防误拼/防注入。
func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}
