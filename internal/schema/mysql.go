package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"gocdc/internal/model"
)

// MySQLLoader 经 information_schema 加载表结构（03：自查方案，
// 不依赖 go-mysql canal schema 包——类型归一化需要 COLUMN_TYPE 原文）。
type MySQLLoader struct {
	DB *sql.DB
}

// NewMySQLLoader 构造（DB 为目标源库连接）。
func NewMySQLLoader(db *sql.DB) *MySQLLoader { return &MySQLLoader{DB: db} }

// Load 查询并归一化一张表（列序 = ORDINAL_POSITION，与 binlog RowsEvent 列序一致）。
func (l *MySQLLoader) Load(ctx context.Context, schemaName, table string) (*model.TableMeta, error) {
	if schemaName == "" {
		return nil, ErrEmptySchema
	}
	if table == "" {
		return nil, ErrEmptyTable
	}

	cols, err := l.loadColumns(ctx, schemaName, table)
	if err != nil {
		return nil, err
	}
	pk, err := l.loadPK(ctx, schemaName, table)
	if err != nil {
		return nil, err
	}
	uniqueIndexes, err := l.loadUniqueIndexes(ctx, schemaName, table)
	if err != nil {
		return nil, err
	}
	comment, err := l.loadComment(ctx, schemaName, table)
	if err != nil {
		return nil, err
	}

	meta := &model.TableMeta{
		Schema:              schemaName,
		Table:               table,
		Columns:             cols,
		PKColumns:           pk,
		UniqueIndexes:       uniqueIndexes,
		UniqueIndexesLoaded: true,
		Comment:             comment,
	}
	for i := range meta.Columns {
		for _, pkName := range pk {
			if meta.Columns[i].Name == pkName {
				meta.Columns[i].IsPK = true
				break
			}
		}
	}
	return meta, nil
}

func (l *MySQLLoader) loadColumns(ctx context.Context, schemaName, table string) ([]model.Column, error) {
	const q = `SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, CHARACTER_SET_NAME,
	       NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_COMMENT
	       FROM information_schema.COLUMNS
	       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	       ORDER BY ORDINAL_POSITION`
	rows, err := l.DB.QueryContext(ctx, q, schemaName, table)
	if err != nil {
		return nil, fmt.Errorf("schema: 查询 %s.%s 列失败: %w", schemaName, table, err)
	}
	defer rows.Close()

	var (
		out []model.Column
		any bool
	)
	for rows.Next() {
		var (
			name, dataType, colType string
			charset, comment        sql.NullString
			numericPrec, numericScl sql.NullInt64
		)
		if err := rows.Scan(&name, &dataType, &colType, &charset, &numericPrec, &numericScl, &comment); err != nil {
			return nil, fmt.Errorf("schema: 扫描 %s.%s 列失败: %w", schemaName, table, err)
		}
		any = true
		p := parseColumnType(colType)
		c := model.Column{
			Name:     name,
			Type:     dataType, // information_schema 规范化类型名，与 model 常量词汇表一致
			RawType:  colType,
			Unsigned: p.unsigned,
			Charset:  charset.String,
			Comment:  comment.String,
		}
		applyPrecision(&c, p, numericPrec, numericScl)
		if dataType == model.TypeEnum {
			c.EnumValues = p.vals
		}
		if dataType == model.TypeSet {
			c.SetValues = p.vals
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("schema: 遍历 %s.%s 列失败: %w", schemaName, table, err)
	}
	if !any {
		return nil, ErrTableNotFound
	}
	return out, nil
}

// applyPrecision 精度/标度策略：decimal 取 COLUMN_TYPE 的 (M,D)；bit 取位数；
// varchar/char/binary/varbinary 取长度；datetime/timestamp/time 取 fsp；
// numeric 系列回退 NUMERIC_PRECISION/SCALE（防御 COLUMN_TYPE 缺括号的老版本）。
func applyPrecision(c *model.Column, p parsed, numericPrec, numericScl sql.NullInt64) {
	switch c.Type {
	case model.TypeDecimal:
		if p.hasLen {
			c.Precision, c.Scale = p.m1, p.m2
			return
		}
		c.Precision, c.Scale = int(numericPrec.Int64), int(numericScl.Int64)
	case model.TypeBit, model.TypeVarchar, model.TypeChar,
		model.TypeBinary, model.TypeVarbinary,
		model.TypeDatetime, model.TypeTimestamp, model.TypeTime:
		if p.hasLen {
			c.Precision = p.m1
		}
	}
}

func (l *MySQLLoader) loadPK(ctx context.Context, schemaName, table string) ([]string, error) {
	// KEY_COLUMN_USAGE 按 ORDINAL_POSITION 排序：联合主键的列序
	const q = `SELECT ku.COLUMN_NAME
	       FROM information_schema.KEY_COLUMN_USAGE ku
	       WHERE ku.TABLE_SCHEMA = ? AND ku.TABLE_NAME = ? AND ku.CONSTRAINT_NAME = 'PRIMARY'
	       ORDER BY ku.ORDINAL_POSITION`
	rows, err := l.DB.QueryContext(ctx, q, schemaName, table)
	if err != nil {
		return nil, fmt.Errorf("schema: 查询 %s.%s 主键失败: %w", schemaName, table, err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("schema: 扫描 %s.%s 主键失败: %w", schemaName, table, err)
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// loadUniqueIndexes 读取主键与全部唯一索引，列序/前缀长度参与语义匹配。
// 函数索引的 COLUMN_NAME 为 NULL，保留标记由启动核对层 fail-fast 处理。
func (l *MySQLLoader) loadUniqueIndexes(ctx context.Context, schemaName, table string) ([]model.UniqueIndex, error) {
	const q = `SELECT INDEX_NAME, COLUMN_NAME, SUB_PART
	       FROM information_schema.STATISTICS
	       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0
	       ORDER BY INDEX_NAME, SEQ_IN_INDEX`
	rows, err := l.DB.QueryContext(ctx, q, schemaName, table)
	if err != nil {
		return nil, fmt.Errorf("schema: 查询 %s.%s 唯一索引失败: %w", schemaName, table, err)
	}
	defer rows.Close()

	var out []model.UniqueIndex
	byName := make(map[string]int)
	for rows.Next() {
		var name string
		var column sql.NullString
		var prefix sql.NullInt64
		if err := rows.Scan(&name, &column, &prefix); err != nil {
			return nil, fmt.Errorf("schema: 扫描 %s.%s 唯一索引失败: %w", schemaName, table, err)
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
		return nil, fmt.Errorf("schema: 遍历 %s.%s 唯一索引失败: %w", schemaName, table, err)
	}
	return out, nil
}

func (l *MySQLLoader) loadComment(ctx context.Context, schemaName, table string) (string, error) {
	const q = `SELECT TABLE_COMMENT FROM information_schema.TABLES
	       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`
	var comment sql.NullString
	if err := l.DB.QueryRowContext(ctx, q, schemaName, table).Scan(&comment); err != nil {
		if err == sql.ErrNoRows {
			return "", ErrTableNotFound
		}
		return "", fmt.Errorf("schema: 查询 %s.%s 表注释失败: %w", schemaName, table, err)
	}
	return strings.TrimSpace(comment.String), nil
}
