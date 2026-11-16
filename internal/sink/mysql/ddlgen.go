package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gocdc/internal/model"
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
	KindAddColumn      = "add_column"       // 加列（白名单）
	KindModifySafe     = "modify_safe"      // 列安全属性：扩容/默认值/可空/注释（白名单）
	KindModifyColumn   = "modify_column"    // 列类型变更/缩容（白名单外）
	KindRenameColumn   = "rename_column"    // 列改名：按位置+类型推断（白名单外）
	KindDropColumn     = "drop_column"      // 删列（白名单外）
	KindDropAutoInc    = "drop_autoinc"     // 去除自增（主键变更前置，白名单外）
	KindPKChange       = "pk_change"        // 主键变更（白名单外）
	KindRestoreAutoInc = "restore_autoinc"  // 恢复自增（主键变更后置，白名单外）
	KindTableComment   = "table_comment"    // 表注释（白名单）
	KindCreateTable    = "create_table"     // 建表（白名单）
	KindAddUniqueIndex = "add_unique_index" // 补建唯一索引（白名单外；2026-11-10 重启拒启陷阱修复）
	KindDropIndex      = "drop_index"       // 删目标多余唯一索引（白名单外）
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
	Unique  []model.UniqueIndex // 唯一索引（不含 PRIMARY；签名比对用）
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

	// ── 唯一索引（2026-11-10 重启拒启陷阱修复：源端 ADD UNIQUE INDEX 若不参与
	// 对账，目标永远缺失 → 下次启动被 VerifyTargetTable fail-fast 拒启。签名比对
	// 同启动核对口径；非白名单——大表建索引可能耗时，挂起审批执行。表达式索引
	// 无法生成语句（启动核对同样拒绝），跳过生成由重启核对兜底）──
	if !uniqueSameSet(src.Unique, tgt.Unique) {
		tgtSig := map[string]bool{}
		for _, idx := range tgt.Unique {
			tgtSig[uniqueIndexSignature(idx)] = true
		}
		srcSig := map[string]bool{}
		for _, idx := range src.Unique {
			srcSig[uniqueIndexSignature(idx)] = true
		}
		for _, idx := range src.Unique {
			if tgtSig[uniqueIndexSignature(idx)] || hasExpressionPart(idx) {
				continue
			}
			add(alter("ADD UNIQUE INDEX %s (%s)", quote(idx.Name), indexPartsSQL(idx)),
				KindAddUniqueIndex,
				"补建唯一索引 "+idx.Name+" "+describeUniqueIndex(idx)+"（源端已建）", false)
		}
		for _, idx := range tgt.Unique {
			if srcSig[uniqueIndexSignature(idx)] || hasExpressionPart(idx) {
				continue
			}
			add(alter("DROP INDEX %s", quote(idx.Name)), KindDropIndex,
				"删目标多余唯一索引 "+idx.Name+" "+describeUniqueIndex(idx)+"（源端已删）", false)
		}
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
		// 前置：旧主键列含自增且退出新主键 → 先去掉（MySQL 要求自增列恒为键；
		// 单独 DROP PRIMARY KEY 会对仍带 AUTO_INCREMENT 的列报 1075）
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
		// DROP+ADD 必须合并为单条 ALTER：AUTO_INCREMENT 列即使保留在新主键里，
		// 分开执行 DROP PRIMARY KEY 的瞬间自增列无键 → ERROR 1075（原子子句
		// 列表以语句终态校验，2026-11-10 review 修复）
		if len(tgt.PK) > 0 && len(src.PK) > 0 {
			pqs := make([]string, 0, len(src.PK))
			for _, p := range src.PK {
				pqs = append(pqs, quote(p))
			}
			add(alter("DROP PRIMARY KEY, ADD PRIMARY KEY (%s)", strings.Join(pqs, ",")),
				KindPKChange,
				"主键变更 ["+strings.Join(tgt.PK, ",")+"] → ["+strings.Join(src.PK, ",")+"]（单条原子 ALTER）",
				false)
		} else if len(tgt.PK) > 0 {
			add(alter("DROP PRIMARY KEY"), KindPKChange,
				"删主键 ["+strings.Join(tgt.PK, ",")+"]（源端无主键）", false)
		} else if len(src.PK) > 0 {
			pqs := make([]string, 0, len(src.PK))
			for _, p := range src.PK {
				pqs = append(pqs, quote(p))
			}
			add(alter("ADD PRIMARY KEY (%s)", strings.Join(pqs, ",")), KindPKChange,
				"建主键 ["+strings.Join(src.PK, ",")+"]", false)
		}
		// 后置：新主键列在源端是自增而目标不是 → 恢复自增（此时新主键已就位，
		// 自增列已是键的一部分，单独 MODIFY 合法）
		for _, pkc := range src.PK {
			sc := srcByName[pkc]
			tc, ok := tgtByName[pkc]
			if ok && sc.AutoInc && !tc.AutoInc {
				add(alter("MODIFY COLUMN %s", colDefSQL(sc)), KindRestoreAutoInc,
					fmt.Sprintf("列 %s 恢复 AUTO_INCREMENT（随源端定义）", pkc), false)
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

// uniqueSameSet 两端唯一索引签名集合是否一致（顺序无关）。
func uniqueSameSet(a, b []model.UniqueIndex) bool {
	if len(a) != len(b) {
		return false
	}
	set := map[string]bool{}
	for _, idx := range a {
		set[uniqueIndexSignature(idx)] = true
	}
	for _, idx := range b {
		if !set[uniqueIndexSignature(idx)] {
			return false
		}
	}
	return true
}

// indexPartsSQL 索引列清单 SQL（带前缀长度：col / col(8)）。
func indexPartsSQL(idx model.UniqueIndex) string {
	parts := make([]string, len(idx.Parts))
	for i, p := range idx.Parts {
		if p.PrefixLength > 0 {
			parts[i] = fmt.Sprintf("`%s`(%d)", strings.ReplaceAll(p.Column, "`", "``"), p.PrefixLength)
		} else {
			parts[i] = "`" + strings.ReplaceAll(p.Column, "`", "``") + "`"
		}
	}
	return strings.Join(parts, ",")
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

	// 唯一索引（不含 PRIMARY——主键单独对账；启动核对 VerifyTargetTable 同口径）
	ixRows, err := db.QueryContext(ctx, `SELECT INDEX_NAME, COLUMN_NAME, SUB_PART
	       FROM information_schema.STATISTICS
	       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0 AND INDEX_NAME <> 'PRIMARY'
	       ORDER BY INDEX_NAME, SEQ_IN_INDEX`, schemaName, table)
	if err != nil {
		return nil, err
	}
	defer ixRows.Close()
	byName := map[string]int{}
	for ixRows.Next() {
		var name string
		var column sql.NullString
		var prefix sql.NullInt64
		if err := ixRows.Scan(&name, &column, &prefix); err != nil {
			return nil, err
		}
		i, ok := byName[name]
		if !ok {
			i = len(def.Unique)
			byName[name] = i
			def.Unique = append(def.Unique, model.UniqueIndex{Name: name})
		}
		part := model.UniqueIndexPart{Column: column.String, Expression: !column.Valid}
		if prefix.Valid {
			part.PrefixLength = int(prefix.Int64)
		}
		def.Unique[i].Parts = append(def.Unique[i].Parts, part)
	}
	if err := ixRows.Err(); err != nil {
		return nil, err
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
		case (c.ColumnType == "timestamp" || c.ColumnType == "datetime") && isTimestampFuncDefault(d):
			// 函数型默认（CURRENT_TIMESTAMP / CURRENT_TIMESTAMP(fsp)）不加引号
			b.WriteString(" DEFAULT " + d)
		default:
			// 其余一律加引号（含 datetime/timestamp 的字面量默认值，如
			// "2020-01-01 00:00:00"——information_schema 返回时不带引号，
			// 原样拼接会生成 DEFAULT 2020-01-01 00:00:00 非法 SQL，
			// 2026-11-16 review 二轮）
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
	for _, idx := range src.Unique {
		if hasExpressionPart(idx) {
			continue
		}
		fmt.Fprintf(&b, ", UNIQUE KEY %s (%s)", quote(idx.Name), indexPartsSQL(idx))
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

// isTimestampFuncDefault 默认值是否为 CURRENT_TIMESTAMP 函数形态（含 fsp 精度）。
// information_schema.COLUMN_DEFAULT 对函数型默认返回 "CURRENT_TIMESTAMP" 或
// "CURRENT_TIMESTAMP(6)"（小写），对字面量默认返回不带引号的裸字面量——后者必须
// 加引号拼接，前者原样（2026-11-16 review 二轮）。
func isTimestampFuncDefault(d string) bool {
	return strings.HasPrefix(strings.ToUpper(d), "CURRENT_TIMESTAMP")
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
