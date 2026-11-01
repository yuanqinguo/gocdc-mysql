package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"gocdc/internal/model"
)

// TargetCheckResult 目标表结构核对结果（05 §5.1 / 10 §3 启动检查）。
// Errors 非空 = fail-fast（拒绝启动）；Warnings 人工关注但不阻塞。
type TargetCheckResult struct {
	Errors   []string
	Warnings []string
}

// OK 是否可启动。
func (r *TargetCheckResult) OK() bool { return len(r.Errors) == 0 }

// VerifyTargetTable 目标表结构核对（05 §5.1.4）：
//  1. 目标表存在且包含全部非 exclude 列（列缺失写入必炸 → fail-fast）；
//  2. exclude 列仍存在目标表 → 告警（10 §3：目标列取 DEFAULT/NULL）；该列
//     NOT NULL 且无默认 → fail-fast（10 §3 明确拒绝）；
//  3. 主键/伪主键（order_by_column）必须存在于目标表主键或唯一索引的最左前缀
//     （delete-by-pk / upsert 寻址的前提）；mask 列参与唯一键 → 告警（05 §5.1.3
//     脱敏碰撞合并行风险）；exclude 列参与目标唯一键 → fail-fast（10 §3）。
func VerifyTargetTable(ctx context.Context, db *sql.DB, targetDB string,
	meta *model.TableMeta, exclude []string, maskCols map[string]bool,
	pseudoPK string) *TargetCheckResult {

	res := &TargetCheckResult{}
	addErr := func(format string, a ...any) { res.Errors = append(res.Errors, fmt.Sprintf(format, a...)) }
	addWarn := func(format string, a ...any) { res.Warnings = append(res.Warnings, fmt.Sprintf(format, a...)) }

	excluded := map[string]bool{}
	for _, c := range exclude {
		excluded[c] = true
	}

	// ── 目标表存在性 + 列集 ──
	cols, err := loadTargetColumns(ctx, db, targetDB, meta.Table)
	if err != nil {
		addErr("目标表 %s.%s 核对失败: %v", targetDB, meta.Table, err)
		return res
	}
	if len(cols) == 0 {
		addErr("目标表 %s.%s 不存在（目标表须预先存在；DDL 建表走 ddl_policy=auto，08 §2）", targetDB, meta.Table)
		return res
	}

	for i := range meta.Columns {
		c := &meta.Columns[i]
		if excluded[c.Name] {
			tc, ok := cols[c.Name]
			if !ok {
				continue // 目标无此列：exclude 的正常形态（10 §3 ✅）
			}
			if tc.notNull && !tc.hasDefault && !tc.autoInc {
				addErr("目标表 %s.%s 列 %s 为 exclude 列且 NOT NULL 无默认值（写入必失败，10 §3 fail-fast）",
					targetDB, meta.Table, c.Name)
			} else {
				addWarn("目标表 %s.%s 仍包含 exclude 列 %s（写入将取 DEFAULT/NULL；建议目标表同步删列，10 §3）",
					targetDB, meta.Table, c.Name)
			}
			continue
		}
		if _, ok := cols[c.Name]; !ok {
			addErr("目标表 %s.%s 缺少源列 %s（列集须一一对应，05 §5.1.2）", targetDB, meta.Table, c.Name)
		}
	}

	// ── 主键/唯一键参与情况 ──
	pkCols := meta.PKColumns
	if len(pkCols) == 0 && pseudoPK != "" {
		pkCols = []string{pseudoPK}
	}
	if len(pkCols) == 0 {
		addErr("表 %s.%s 无主键且规则未配 order_by_column（05 §5.1.3，无法寻址）", meta.Schema, meta.Table)
		return res
	}

	uks, err := loadTargetUniqueIndexes(ctx, db, targetDB, meta.Table)
	if err != nil {
		addErr("目标表 %s.%s 唯一键核对失败: %v", targetDB, meta.Table, err)
		return res
	}

	for _, uq := range uks {
		for _, part := range uq.Parts {
			if part.Expression {
				addErr("目标表 %s.%s 含函数唯一索引 %s，当前无法安全比较源/目标唯一约束",
					targetDB, meta.Table, uq.Name)
				continue
			}
			col := part.Column
			if excluded[col] {
				addErr("exclude 列 %s 参与目标表 %s.%s 唯一键 %s（破坏幂等寻址，10 §3 fail-fast）",
					col, targetDB, meta.Table, describeUniqueIndex(uq))
			}
			if maskCols[col] {
				addWarn("mask 列 %s 参与目标表 %s.%s 唯一键 %s（脱敏碰撞可能合并行，05 §5.1.3）",
					col, targetDB, meta.Table, describeUniqueIndex(uq))
			}
		}
	}

	if !uniqueCovers(uks, pkCols) {
		addErr("目标表 %s.%s 的主键/唯一键均不覆盖寻址键 (%s)（delete/upsert 无法定位，05 §5.1.3）",
			targetDB, meta.Table, strings.Join(pkCols, ","))
	}
	if !meta.UniqueIndexesLoaded {
		addErr("源表 %s.%s 唯一索引元数据未加载，拒绝启动以避免目标唯一键冲突静默覆盖", meta.Schema, meta.Table)
	} else {
		sourceIndexes := make([]model.UniqueIndex, 0, len(meta.UniqueIndexes))
		for _, idx := range meta.UniqueIndexes {
			// exclude 列不会写入目标；含 exclude 列的源约束不要求目标镜像。
			if indexHasExcluded(idx, excluded) {
				continue
			}
			if hasExpressionPart(idx) {
				addErr("源表 %s.%s 含函数唯一索引 %s，当前无法安全比较源/目标唯一约束",
					meta.Schema, meta.Table, idx.Name)
				continue
			}
			sourceIndexes = append(sourceIndexes, idx)
		}
		targetIndexes := make([]model.UniqueIndex, 0, len(uks))
		for _, idx := range uks {
			if !indexHasExcluded(idx, excluded) {
				targetIndexes = append(targetIndexes, idx)
			}
		}
		missing, extra := uniqueIndexDiff(sourceIndexes, targetIndexes)
		for _, idx := range missing {
			addErr("目标表 %s.%s 缺少源端唯一键 %s", targetDB, meta.Table, idx)
		}
		for _, idx := range extra {
			addErr("目标表 %s.%s 存在源端没有的唯一键 %s；ON DUPLICATE KEY UPDATE 可能折叠不同主键行",
				targetDB, meta.Table, idx)
		}
	}
	return res
}

// targetCol 目标列的核对所需元数据。
type targetCol struct {
	notNull    bool
	hasDefault bool
	autoInc    bool
}

func loadTargetColumns(ctx context.Context, db *sql.DB, targetDB, table string) (map[string]targetCol, error) {
	const q = `SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
	       FROM information_schema.COLUMNS
	       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`
	rows, err := db.QueryContext(ctx, q, targetDB, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]targetCol{}
	for rows.Next() {
		var name, nullable string
		var def, extra sql.NullString
		if err := rows.Scan(&name, &nullable, &def, &extra); err != nil {
			return nil, err
		}
		out[name] = targetCol{
			notNull:    nullable == "NO",
			hasDefault: def.Valid,
			autoInc:    strings.Contains(strings.ToLower(extra.String), "auto_increment"),
		}
	}
	return out, rows.Err()
}

// loadTargetUniqueIndexes 返回目标表全部唯一索引（含主键），索引名不参与语义比较。
func loadTargetUniqueIndexes(ctx context.Context, db *sql.DB, targetDB, table string) ([]model.UniqueIndex, error) {
	const q = `SELECT INDEX_NAME, COLUMN_NAME, SUB_PART
	       FROM information_schema.STATISTICS
	       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0
	       ORDER BY INDEX_NAME, SEQ_IN_INDEX`
	rows, err := db.QueryContext(ctx, q, targetDB, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]model.UniqueIndex, 0)
	byName := map[string]int{}
	for rows.Next() {
		var name string
		var column sql.NullString
		var prefix sql.NullInt64
		if err := rows.Scan(&name, &column, &prefix); err != nil {
			return nil, err
		}
		i, ok := byName[name]
		if !ok {
			i = len(out)
			byName[name] = i
			out = append(out, model.UniqueIndex{Name: name})
		}
		part := model.UniqueIndexPart{Column: column.String, Expression: !column.Valid}
		if prefix.Valid {
			part.PrefixLength = int(prefix.Int64)
		}
		out[i].Parts = append(out[i].Parts, part)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func uniqueCovers(uks []model.UniqueIndex, pkCols []string) bool {
	for _, uq := range uks {
		if len(uq.Parts) < len(pkCols) {
			continue
		}
		ok := true
		for i, c := range pkCols {
			if uq.Parts[i].Expression || uq.Parts[i].PrefixLength != 0 || uq.Parts[i].Column != c {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func indexHasExcluded(idx model.UniqueIndex, excluded map[string]bool) bool {
	for _, part := range idx.Parts {
		if excluded[part.Column] {
			return true
		}
	}
	return false
}

func hasExpressionPart(idx model.UniqueIndex) bool {
	for _, part := range idx.Parts {
		if part.Expression {
			return true
		}
	}
	return false
}

func uniqueIndexSignature(idx model.UniqueIndex) string {
	var b strings.Builder
	for _, part := range idx.Parts {
		fmt.Fprintf(&b, "%d:%s:%d:%t;", len(part.Column), part.Column, part.PrefixLength, part.Expression)
	}
	return b.String()
}

func describeUniqueIndex(idx model.UniqueIndex) string {
	parts := make([]string, len(idx.Parts))
	for i, part := range idx.Parts {
		if part.Expression {
			parts[i] = "<expression>"
		} else if part.PrefixLength > 0 {
			parts[i] = fmt.Sprintf("%s(%d)", part.Column, part.PrefixLength)
		} else {
			parts[i] = part.Column
		}
	}
	return "(" + strings.Join(parts, ",") + ")"
}

// uniqueIndexDiff 比较唯一索引签名（有序列名/前缀长度；忽略索引名称）。
func uniqueIndexDiff(source, target []model.UniqueIndex) (missing, extra []string) {
	targetCounts := make(map[string]int, len(target))
	for _, idx := range target {
		targetCounts[uniqueIndexSignature(idx)]++
	}
	for _, idx := range source {
		sig := uniqueIndexSignature(idx)
		if targetCounts[sig] > 0 {
			targetCounts[sig]--
		} else {
			missing = append(missing, describeUniqueIndex(idx))
		}
	}
	sourceCounts := make(map[string]int, len(source))
	for _, idx := range source {
		sourceCounts[uniqueIndexSignature(idx)]++
	}
	for _, idx := range target {
		sig := uniqueIndexSignature(idx)
		if sourceCounts[sig] > 0 {
			sourceCounts[sig]--
		} else {
			extra = append(extra, describeUniqueIndex(idx))
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	return missing, extra
}
