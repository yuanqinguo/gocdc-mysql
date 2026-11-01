package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrSourceTableGone diff 时源表已不存在（审批挂起期间被后续 DDL 删除/改名）。
// 调用方（pipeline worker）据此判定该 DDL 事件已过期：按 skip 放行继续处理
// 后续事件，而非 fatal（CREATE 审批挂起期间源表可能被 RENAME，approve
// 复验报「源表不存在」，整条管线退出无意义）。
var ErrSourceTableGone = errors.New("ddlgen: 源表已不存在（DDL 已过期）")

// DDL 白名单自动同步（08 §2）：
//   ADD COLUMN / varchar 长度放大 / 注释变更 / CREATE TABLE 自动生成目标端 DDL
//   （列定义取自源 information_schema，含注释/默认值）；其余差异
//   （DROP/RENAME/类型变更/缩容/主键变更…）不在白名单 → 调用方转 pause。
//
// 判定方式：不解析 DDL SQL，而是**对比源与目标 information_schema**——差异集合
// ⊆ 白名单操作才放行（对 ANY 语句形态鲁棒，且执行后可复验）。

// ColDef 源/目标列的完整定义（生成/比对 DDL 用）。
type ColDef struct {
	Name       string
	ColumnType string // information_schema.DATA_TYPE（小写）
	RawType    string // COLUMN_TYPE（如 varchar(64)、decimal(10,2)）
	Nullable   bool
	Default    sql.NullString
	AutoInc    bool
	Comment    string
	Charset    string
}

// DiffResult 源→目标的结构差异。
type DiffResult struct {
	Stmts       []string // 白名单内的目标端 DDL（空 = 已一致）
	Whitelisted bool     // 差异是否全部落在白名单（false = 有白名单外差异，需 pause）
	CreateTable bool     // 目标表缺失（CREATE TABLE 场景）
	Summary     string   // 人读摘要（审计/告警）
}

// DiffTargetTable 对比源表与目标表，产出白名单 DDL（08 §2/§3-6a）。
// srcDB/tgtDB 为源/目标连接；srcSchema 为源库名，tgtSchema 为目标库名（同名同表约束，
// 表名一致）。
func DiffTargetTable(ctx context.Context, srcDB, tgtDB *sql.DB,
	srcSchema, tgtSchema, table string) (*DiffResult, error) {

	srcCols, srcPK, srcComment, err := loadTableDef(ctx, srcDB, srcSchema, table)
	if err != nil {
		return nil, fmt.Errorf("ddlgen: 源表 %s.%s: %w", srcSchema, table, err)
	}
	if len(srcCols) == 0 {
		return nil, fmt.Errorf("%w: %s.%s", ErrSourceTableGone, srcSchema, table)
	}
	tgtCols, tgtPK, tgtComment, err := loadTableDef(ctx, tgtDB, tgtSchema, table)
	if err != nil {
		return nil, fmt.Errorf("ddlgen: 目标表 %s.%s: %w", tgtSchema, table, err)
	}

	res := &DiffResult{}
	quote := func(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

	// 目标表缺失 → CREATE TABLE（白名单：结构含主键/注释，08 §2）
	if len(tgtCols) == 0 {
		res.CreateTable = true
		res.Whitelisted = true
		res.Stmts = []string{buildCreateTable(tgtSchema, table, srcCols, srcPK, srcComment)}
		res.Summary = "目标表缺失 → CREATE TABLE（" + fmt.Sprint(len(srcCols)) + " 列）"
		return res, nil
	}

	var stmts []string
	var outside []string

	// 主键差异：一律白名单外（改主键危险，08 §2）
	if strings.Join(srcPK, ",") != strings.Join(tgtPK, ",") {
		outside = append(outside, "主键变更: ["+strings.Join(tgtPK, ",")+"] → ["+strings.Join(srcPK, ",")+"]")
	}

	for i := range srcCols {
		s := srcCols[i]
		t, ok := tgtCols[s.Name]
		if !ok {
			// ADD COLUMN（追加在末尾；列序差异由 sink 按列名对齐消化，08 §3）
			stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s",
				quote(tgtSchema), quote(table), colDefSQL(s)))
			continue
		}
		// varchar 长度放大（同基类型且都为 varchar/char 且目标更短）
		if s.ColumnType == t.ColumnType && isVarcharFamily(s.ColumnType) && widthOf(s.RawType) > widthOf(t.RawType) {
			stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s",
				quote(tgtSchema), quote(table), colDefSQL(s)))
			continue
		}
		// 仅注释变更
		if s.RawType == t.RawType && s.Comment != t.Comment {
			stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s",
				quote(tgtSchema), quote(table), colDefSQL(s)))
			continue
		}
		// 其余类型/默认值差异：类型变更不在白名单
		if s.RawType != t.RawType || s.ColumnType != t.ColumnType {
			outside = append(outside, fmt.Sprintf("列 %s 类型变更: %s → %s", s.Name, t.RawType, s.RawType))
		}
	}
	for i := range tgtCols {
		if _, ok := srcCols[tgtCols[i].Name]; !ok {
			outside = append(outside, fmt.Sprintf("列 %s 在源端不存在（疑似 DROP/RENAME）", tgtCols[i].Name))
		}
	}
	if srcComment != tgtComment {
		stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s.%s COMMENT='%s'",
			quote(tgtSchema), quote(table), strings.ReplaceAll(srcComment, "'", "''")))
	}

	res.Stmts = stmts
	res.Whitelisted = len(outside) == 0
	res.Summary = fmt.Sprintf("白名单 DDL %d 条；白名单外差异 %d 项", len(stmts), len(outside))
	if len(outside) > 0 {
		res.Summary += "（" + strings.Join(outside, "; ") + "）"
	}
	return res, nil
}

// ApplyTargetDDL 执行目标端 DDL（每条 60s 超时，08 §3：超时 → 调用方 pause）。
func ApplyTargetDDL(ctx context.Context, db *sql.DB, stmts []string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	for _, stmt := range stmts {
		actx, cancel := context.WithTimeout(ctx, timeout)
		if _, err := db.ExecContext(actx, stmt); err != nil {
			cancel()
			return fmt.Errorf("ddlgen: 执行目标 DDL 失败 [%s]: %w", stmt, err)
		}
		cancel()
	}
	return nil
}

// ── information_schema 读取 ─────────────────────────────────────────────

func loadTableDef(ctx context.Context, db *sql.DB, schemaName, table string) (
	map[string]ColDef, []string, string, error) {
	const q = `SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT,
	       EXTRA, COLUMN_COMMENT
	       FROM information_schema.COLUMNS
	       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	       ORDER BY ORDINAL_POSITION`
	rows, err := db.QueryContext(ctx, q, schemaName, table)
	if err != nil {
		return nil, nil, "", err
	}
	defer rows.Close()
	cols := map[string]ColDef{}
	var order []string
	for rows.Next() {
		var c ColDef
		var nullable, extra sql.NullString
		if err := rows.Scan(&c.Name, &c.ColumnType, &c.RawType, &nullable, &c.Default, &extra, &c.Comment); err != nil {
			return nil, nil, "", err
		}
		c.Nullable = nullable.String == "YES"
		c.AutoInc = strings.Contains(strings.ToLower(extra.String), "auto_increment")
		cols[c.Name] = c
		order = append(order, c.Name)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, "", err
	}
	if len(order) == 0 {
		return cols, nil, "", nil // 表不存在
	}

	var pk []string
	pkRows, err := db.QueryContext(ctx, `SELECT ku.COLUMN_NAME
	       FROM information_schema.KEY_COLUMN_USAGE ku
	       WHERE ku.TABLE_SCHEMA = ? AND ku.TABLE_NAME = ? AND ku.CONSTRAINT_NAME = 'PRIMARY'
	       ORDER BY ku.ORDINAL_POSITION`, schemaName, table)
	if err != nil {
		return nil, nil, "", err
	}
	defer pkRows.Close()
	for pkRows.Next() {
		var name string
		if err := pkRows.Scan(&name); err != nil {
			return nil, nil, "", err
		}
		pk = append(pk, name)
	}

	var comment string
	if err := db.QueryRowContext(ctx, `SELECT TABLE_COMMENT FROM information_schema.TABLES
	       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`, schemaName, table).Scan(&comment); err != nil && err != sql.ErrNoRows {
		return nil, nil, "", err
	}
	return cols, pk, strings.TrimSpace(comment), nil
}

// colDefSQL 生成列定义（含 NOT NULL/DEFAULT/AUTO_INCREMENT/COMMENT）。
func colDefSQL(c ColDef) string {
	var b strings.Builder
	fmt.Fprintf(&b, "`%s` %s", strings.ReplaceAll(c.Name, "`", "``"), c.RawType)
	if !c.Nullable && !c.AutoInc {
		b.WriteString(" NOT NULL")
	}
	if c.Default.Valid {
		d := c.Default.String
		switch {
		case d == "NULL":
			b.WriteString(" DEFAULT NULL")
		case c.ColumnType == "timestamp" || c.ColumnType == "datetime":
			b.WriteString(" DEFAULT " + d) // CURRENT_TIMESTAMP 等字面量
		case isNumericType(c.ColumnType) || isVarcharFamily(c.ColumnType):
			b.WriteString(" DEFAULT '" + strings.ReplaceAll(d, "'", "''") + "'")
		default:
			b.WriteString(" DEFAULT '" + strings.ReplaceAll(d, "'", "''") + "'")
		}
	}
	if c.AutoInc {
		b.WriteString(" AUTO_INCREMENT")
	}
	if c.Comment != "" {
		b.WriteString(" COMMENT '" + strings.ReplaceAll(c.Comment, "'", "''") + "'")
	}
	return b.String()
}

// buildCreateTable 生成建表语句（白名单 CREATE TABLE；含主键/注释，08 §2）。
func buildCreateTable(schemaName, table string, cols map[string]ColDef, pk []string, comment string) string {
	quote := func(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
	// 保持列序：map 无序，按 information_schema 惯例重建需有序清单——
	// 调用方从 loadTableDef 的 order 语义而来；此处用列名排序近似（结构等价）
	names := make([]string, 0, len(cols))
	for n := range cols {
		names = append(names, n)
	}
	sortStringsDDL(names)
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s.%s (", quote(schemaName), quote(table))
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(colDefSQL(cols[n]))
	}
	if len(pk) > 0 {
		pqs := make([]string, 0, len(pk))
		for _, p := range pk {
			pqs = append(pqs, quote(p))
		}
		fmt.Fprintf(&b, ", PRIMARY KEY (%s)", strings.Join(pqs, ","))
	}
	b.WriteString(") ENGINE=InnoDB")
	if comment != "" {
		fmt.Fprintf(&b, " COMMENT='%s'", strings.ReplaceAll(comment, "'", "''"))
	}
	return b.String()
}

func sortStringsDDL(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func isVarcharFamily(t string) bool {
	return t == "varchar" || t == "char" || t == "varbinary" || t == "binary"
}

func isNumericType(t string) bool {
	switch t {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint",
		"decimal", "numeric", "float", "double", "year":
		return true
	}
	return false
}

// widthOf 从 rawType 提取第一括号参数（varchar(64) → 64；无括号 → 0）。
func widthOf(rawType string) int {
	open := strings.IndexByte(rawType, '(')
	if open < 0 {
		return 0
	}
	closed := strings.IndexByte(rawType[open:], ')')
	if closed < 0 {
		return 0
	}
	n := 0
	for _, ch := range rawType[open+1 : open+closed] {
		if ch < '0' || ch > '9' {
			return 0
		}
		n = n*10 + int(ch-'0')
	}
	return n
}
