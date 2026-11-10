package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ErrSourceTableGone diff 时源表已不存在（审批挂起期间被后续 DDL 删除/改名）。
// 调用方（pipeline worker）据此判定该 DDL 事件已过期：按 skip 放行继续处理
// 后续事件，而非 fatal（CREATE 审批挂起期间源表可能被 RENAME，approve
// 复验报「源表不存在」，整条管线退出无意义）。
var ErrSourceTableGone = errors.New("ddlgen: 源表已不存在（DDL 已过期）")

// DDL 白名单自动同步（08 §2）：
//   ADD COLUMN / varchar 长度放大 / 列安全属性（默认值/可空/注释）/ 表注释 /
//   CREATE TABLE 自动生成目标端 DDL（列定义取自源 information_schema）。
//
// 判定方式：不解析 DDL SQL，而是**对比源与目标 information_schema**——对 ANY
// 语句形态鲁棒，且执行后可复验。
//
// 2026-11-09 全量对账改造（approve 必炸陷阱修复）：差异不再只分类为「白名单内
// 语句 + 白名单外文字」，而是为**全部**差异生成可执行的目标端语句（删列/改名/
// 类型变更/缩容/主键变更均生成，逐条带白名单标记）：
//   - auto 策略：仅当全部语句白名单内才自动执行（语义不变）；
//   - approve（人工批准）：执行全部语句（08 §5-A「执行目标端 DDL → 复验 →
//     resume」终于成立）——此前非白名单差异无语句可执行，approve 复验必失败
//     fatal，且位点冻在 DDL 前、重启重放命中已决 approve 再 fatal = 死循环。

// 语句种类（DDLStmt.Kind；审计/审批页明细展示）。
const (
	KindAddColumn      = "add_column"      // 加列（白名单）
	KindModifySafe     = "modify_safe"     // 列安全属性：扩容/默认值/可空/注释（白名单）
	KindModifyColumn   = "modify_column"   // 列类型变更/缩容（白名单外）
	KindRenameColumn   = "rename_column"   // 列改名：按位置+类型推断（白名单外）
	KindDropColumn     = "drop_column"     // 删列（白名单外）
	KindDropAutoInc    = "drop_autoinc"    // 去除自增（主键变更前置，白名单外）
	KindPKChange       = "pk_change"       // 主键变更（白名单外）
	KindRestoreAutoInc = "restore_autoinc" // 恢复自增（主键变更后置，白名单外）
	KindTableComment   = "table_comment"   // 表注释（白名单）
	KindCreateTable    = "create_table"    // 建表（白名单）
)

// ColDef 源/目标列的完整定义（生成/比对 DDL 用）。
type ColDef struct {
	Name       string
	Ordinal    int    // ORDINAL_POSITION（1 起）——列改名按位置配对、建表保序
	ColumnType string // information_schema.DATA_TYPE（小写）
	RawType    string // COLUMN_TYPE（如 varchar(64)、decimal(10,2)）
	Nullable   bool
	Default    sql.NullString
	AutoInc    bool
	Comment    string
	Charset    string
}

// DDLStmt 一条目标端对账语句（SQL + 分类 + 白名单标记）。
type DDLStmt struct {
	SQL         string
	Kind        string
	Whitelisted bool
	Why         string // 人读理由（审计/审批页明细）
}

// DiffResult 源→目标的结构差异（全部差异均有对应语句）。
type DiffResult struct {
	Stmts       []DDLStmt // 全部对账语句（执行序；空 = 已一致）
	Whitelisted bool      // 全部语句是否均在白名单内（auto 策略自动执行的判据）
	CreateTable bool      // 目标表缺失（CREATE TABLE 场景）
	Summary     string    // 人读摘要（审计/告警；含逐条明细）
}

// AllStmts 全部对账语句 SQL（approve 人工批准后执行的清单）。
func (r *DiffResult) AllStmts() []string {
	out := make([]string, 0, len(r.Stmts))
	for i := range r.Stmts {
		out = append(out, r.Stmts[i].SQL)
	}
	return out
}

// WhitelistedStmts 仅白名单内语句 SQL（auto 策略自动执行的清单）。
func (r *DiffResult) WhitelistedStmts() []string {
	out := make([]string, 0, len(r.Stmts))
	for i := range r.Stmts {
		if r.Stmts[i].Whitelisted {
			out = append(out, r.Stmts[i].SQL)
		}
	}
	return out
}

// DiffTargetTable 对比源表与目标表，产出全部对账语句（08 §2/§3-6a）。
// srcDB/tgtDB 为源/目标连接；srcSchema 为源库名，tgtSchema 为目标库名（同名同表约束，
// 表名一致）。
func DiffTargetTable(ctx context.Context, srcDB, tgtDB *sql.DB,
	srcSchema, tgtSchema, table string) (*DiffResult, error) {

	src, err := loadTableDef(ctx, srcDB, srcSchema, table)
	if err != nil {
		return nil, fmt.Errorf("ddlgen: 源表 %s.%s: %w", srcSchema, table, err)
	}
	if len(src.Cols) == 0 {
		return nil, fmt.Errorf("%w: %s.%s", ErrSourceTableGone, srcSchema, table)
	}
	tgt, err := loadTableDef(ctx, tgtDB, tgtSchema, table)
	if err != nil {
		return nil, fmt.Errorf("ddlgen: 目标表 %s.%s: %w", tgtSchema, table, err)
	}
	return diffDefs(src, tgt, tgtSchema, table), nil
}

// tableDef 一张表的完整定义（loadTableDef 产物；diffDefs 纯函数输入）。
type tableDef struct {
	Cols    []ColDef // 按 ORDINAL_POSITION 有序
	PK      []string
	Comment string
}

// diffDefs 纯函数：源/目标定义 → 全量对账语句（可单测；不触库）。
//
// 语句执行序：列改名（CHANGE）→ 加列/删列/类型变更（MODIFY）→ 表注释 →
// 主键变更（去自增 → DROP PK → ADD PK → 恢复自增，放最后：列操作完成后再动键）。
//
// 列改名配对启发式：目标独有列 T 与源独有列 S 满足「同 ORDINAL_POSITION 且同
// DATA_TYPE」→ 推断为改名，生成 CHANGE COLUMN T S（数据保留，CHANGE 同时采纳
// 源端新定义——改名+扩容/类型微调一条覆盖）。未配对的源独有列 = 加列、目标独有
// 列 = 删列。误配对（实际是「删 T + 同位加同型 S」）的后果 = T 的旧数据被带进
// S 列，随后源端 S 的新写入按主键 upsert 覆盖；审计明细逐条列出推断依据。
func diffDefs(src, tgt *tableDef, tgtSchema, table string) *DiffResult {
	res := &DiffResult{}
	quote := func(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
	alter := func(format string, a ...any) string {
		return fmt.Sprintf("ALTER TABLE %s.%s ", quote(tgtSchema), quote(table)) + fmt.Sprintf(format, a...)
	}

	// 目标表缺失 → CREATE TABLE（白名单：结构含主键/注释/列序，08 §2）
	if len(tgt.Cols) == 0 {
		res.CreateTable = true
		res.Whitelisted = true
		res.Stmts = []DDLStmt{{
			SQL: buildCreateTable(tgtSchema, table, src), Kind: KindCreateTable, Whitelisted: true,
			Why: fmt.Sprintf("目标表缺失，按源结构建表（%d 列）", len(src.Cols)),
		}}
		res.Summary = res.Stmts[0].Why
		return res
	}

	srcByName := map[string]ColDef{}
	for _, c := range src.Cols {
		srcByName[c.Name] = c
	}
	tgtByName := map[string]ColDef{}
	for _, c := range tgt.Cols {
		tgtByName[c.Name] = c
	}

	var stmts []DDLStmt
	add := func(sqlText, kind, why string, whitelisted bool) {
		stmts = append(stmts, DDLStmt{SQL: sqlText, Kind: kind, Why: why, Whitelisted: whitelisted})
	}

	// ── 列改名配对（同位置 + 同 DATA_TYPE）──
	srcOnly := map[string]ColDef{} // 源独有（配对剩余的走加列）
	for _, c := range src.Cols {
		if _, ok := tgtByName[c.Name]; !ok {
			srcOnly[c.Name] = c
		}
	}
	pairedSrc := map[string]bool{} // 已配对为改名的源列名
	pairedTgt := map[string]bool{} // 已配对为改名的目标列名
	for _, t := range tgt.Cols {   // 目标顺序（ordinal 升序）
		if _, ok := srcByName[t.Name]; ok {
			continue
		}
		// 目标独有列 t：在源独有列中找同位置同类型的配对
		var match *ColDef
		for _, s := range srcOnly {
			if pairedSrc[s.Name] || s.Ordinal != t.Ordinal || s.ColumnType != t.ColumnType {
				continue
			}
			m := s
			match = &m
			break
		}
		if match == nil {
			continue
		}
		pairedSrc[match.Name] = true
		pairedTgt[t.Name] = true
		add(alter("CHANGE COLUMN %s %s", quote(t.Name), colDefSQL(*match)),
			KindRenameColumn,
			fmt.Sprintf("列 %s → %s（改名：同位置 %d + 同类型 %s 推断，数据保留）",
				t.Name, match.Name, t.Ordinal, t.ColumnType),
			false)
	}

	// ── 加列 / 删列 / 类型变更 ──
	for _, s := range src.Cols {
		t, ok := tgtByName[s.Name]
		if !ok {
			if pairedSrc[s.Name] {
				continue // 已按改名处理
			}
			add(alter("ADD COLUMN %s", colDefSQL(s)), KindAddColumn,
				fmt.Sprintf("加列 %s %s", s.Name, s.RawType), true)
			continue
		}
		if s.RawType != t.RawType || s.ColumnType != t.ColumnType {
			// varchar 家族长度放大 = 安全（同基类型且目标更短）
			if s.ColumnType == t.ColumnType && isVarcharFamily(s.ColumnType) &&
				widthOf(s.RawType) > widthOf(t.RawType) {
				add(alter("MODIFY COLUMN %s", colDefSQL(s)), KindModifySafe,
					fmt.Sprintf("列 %s 扩容 %s → %s", s.Name, t.RawType, s.RawType), true)
				continue
			}
			add(alter("MODIFY COLUMN %s", colDefSQL(s)), KindModifyColumn,
				fmt.Sprintf("列 %s 类型变更 %s → %s", s.Name, t.RawType, s.RawType), false)
			continue
		}
		// 同类型：默认值/可空/注释差异 = 安全属性调整
		if s.Nullable != t.Nullable || s.Comment != t.Comment || s.Default != t.Default {
			add(alter("MODIFY COLUMN %s", colDefSQL(s)), KindModifySafe,
				fmt.Sprintf("列 %s 属性调整（可空/默认值/注释）", s.Name), true)
		}
	}
	for _, t := range tgt.Cols {
		if _, ok := srcByName[t.Name]; ok {
			continue
		}
		if pairedTgt[t.Name] {
			continue // 已按改名处理（CHANGE 消费）
		}
		add(alter("DROP COLUMN %s", quote(t.Name)), KindDropColumn,
			fmt.Sprintf("删列 %s（源端已不存在）", t.Name), false)
	}

	// ── 表注释 ──
	if src.Comment != tgt.Comment {
		add(alter("COMMENT='%s'", strings.ReplaceAll(src.Comment, "'", "''")),
			KindTableComment, "表注释变更", true)
	}

	// ── 主键变更（放最后：列操作完成后动键）──
	if strings.Join(src.PK, ",") != strings.Join(tgt.PK, ",") {
		newPK := map[string]bool{}
		for _, c := range src.PK {
			newPK[c] = true
		}
		// 前置：旧主键列含自增 → 先去掉（MySQL 不允许对自增列 DROP PRIMARY KEY）
		for _, pkc := range tgt.PK {
			tc, ok := tgtByName[pkc]
			if !ok || !tc.AutoInc || newPK[pkc] {
				continue // 列已被删/改名（前面语句处理）或仍在主键中 → 无需处理
			}
			noAI := tc
			noAI.AutoInc = false
			add(alter("MODIFY COLUMN %s", colDefSQL(noAI)), KindDropAutoInc,
				fmt.Sprintf("列 %s 去除 AUTO_INCREMENT（旧主键列退出主键前置）", pkc), false)
		}
		if len(tgt.PK) > 0 {
			add(alter("DROP PRIMARY KEY"), KindPKChange,
				"删旧主键 ["+strings.Join(tgt.PK, ",")+"]", false)
		}
		if len(src.PK) > 0 {
			pqs := make([]string, 0, len(src.PK))
			for _, p := range src.PK {
				pqs = append(pqs, quote(p))
			}
			add(alter("ADD PRIMARY KEY (%s)", strings.Join(pqs, ",")), KindPKChange,
				"建新主键 ["+strings.Join(src.PK, ",")+"]", false)
			// 后置：新主键列在源端是自增而目标不是 → 恢复自增（自增列必须是键的一部分）
			for _, pkc := range src.PK {
				sc := srcByName[pkc]
				tc, ok := tgtByName[pkc]
				if ok && sc.AutoInc && !tc.AutoInc {
					add(alter("MODIFY COLUMN %s", colDefSQL(sc)), KindRestoreAutoInc,
						fmt.Sprintf("列 %s 恢复 AUTO_INCREMENT（随源端定义）", pkc), false)
				}
			}
		}
	}

	res.Stmts = stmts
	res.Whitelisted = true
	var parts []string
	for i := range stmts {
		if !stmts[i].Whitelisted {
			res.Whitelisted = false
		}
		parts = append(parts, stmts[i].Why)
	}
	res.Summary = fmt.Sprintf("对账语句 %d 条（白名单内 %d）", len(stmts), countWhitelisted(stmts))
	if len(parts) > 0 {
		res.Summary += "：" + strings.Join(parts, "；")
	} else {
		res.Summary = "已一致（无差异）"
	}
	return res
}

func countWhitelisted(stmts []DDLStmt) int {
	n := 0
	for i := range stmts {
		if stmts[i].Whitelisted {
			n++
		}
	}
	return n
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

func loadTableDef(ctx context.Context, db *sql.DB, schemaName, table string) (*tableDef, error) {
	const q = `SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE,
	       COLUMN_DEFAULT, EXTRA, COLUMN_COMMENT
	       FROM information_schema.COLUMNS
	       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	       ORDER BY ORDINAL_POSITION`
	rows, err := db.QueryContext(ctx, q, schemaName, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	def := &tableDef{}
	for rows.Next() {
		var c ColDef
		var nullable, extra sql.NullString
		if err := rows.Scan(&c.Name, &c.Ordinal, &c.ColumnType, &c.RawType, &nullable, &c.Default, &extra, &c.Comment); err != nil {
			return nil, err
		}
		c.Nullable = nullable.String == "YES"
		c.AutoInc = strings.Contains(strings.ToLower(extra.String), "auto_increment")
		def.Cols = append(def.Cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(def.Cols) == 0 {
		return def, nil // 表不存在（调用方按缺失语义处理）
	}

	pkRows, err := db.QueryContext(ctx, `SELECT ku.COLUMN_NAME
	       FROM information_schema.KEY_COLUMN_USAGE ku
	       WHERE ku.TABLE_SCHEMA = ? AND ku.TABLE_NAME = ? AND ku.CONSTRAINT_NAME = 'PRIMARY'
	       ORDER BY ku.ORDINAL_POSITION`, schemaName, table)
	if err != nil {
		return nil, err
	}
	defer pkRows.Close()
	for pkRows.Next() {
		var name string
		if err := pkRows.Scan(&name); err != nil {
			return nil, err
		}
		def.PK = append(def.PK, name)
	}

	var comment string
	if err := db.QueryRowContext(ctx, `SELECT TABLE_COMMENT FROM information_schema.TABLES
	       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`, schemaName, table).Scan(&comment); err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	def.Comment = strings.TrimSpace(comment)
	return def, nil
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
// 列序保持源表 ORDINAL_POSITION（loadTableDef 有序产物；此处按 Ordinal 排序
// 防御非有序输入——纯函数 diffDefs 可被测试以任意切片序喂入）。
func buildCreateTable(schemaName, table string, src *tableDef) string {
	quote := func(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
	cols := make([]ColDef, len(src.Cols))
	copy(cols, src.Cols)
	sort.Slice(cols, func(i, j int) bool { return cols[i].Ordinal < cols[j].Ordinal })
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s.%s (", quote(schemaName), quote(table))
	for i, c := range cols {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(colDefSQL(c))
	}
	if len(src.PK) > 0 {
		pqs := make([]string, 0, len(src.PK))
		for _, p := range src.PK {
			pqs = append(pqs, quote(p))
		}
		fmt.Fprintf(&b, ", PRIMARY KEY (%s)", strings.Join(pqs, ","))
	}
	b.WriteString(") ENGINE=InnoDB")
	if src.Comment != "" {
		fmt.Fprintf(&b, " COMMENT='%s'", strings.ReplaceAll(src.Comment, "'", "''"))
	}
	return b.String()
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
