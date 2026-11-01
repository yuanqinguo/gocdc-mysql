package logging

import (
	"sort"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// RowRedactor 把原始行列值映射为可安全打印的视图：exclude 列整列删除、
// mask 列以脱敏值替换（10 §5）。实现位于 transform 包（全量/增量共用同一规则），
// 日志侧只依赖接口。
//
// 红线：未过本接口的行列值禁止出现在任何日志里。
type RowRedactor interface {
	Redact(schema, table string, row map[string]any) map[string]any
}

// LogRow 以脱敏视图打印一行数据（调试用；正常行级日志默认关闭，见 11 §3 采样策略）。
//
// redactor 为 nil 或 row 为 nil 时只打印 schema/table 与列名清单，
// 绝不输出任何列值——宁可少打，不可漏脱敏。
func LogRow(l *zap.Logger, level zapcore.Level, redactor RowRedactor, schema, table string, row map[string]any, msg string) {
	if l == nil {
		return
	}
	if row == nil || redactor == nil {
		l.Log(level, msg, zap.String("schema", schema), zap.String("table", table),
			zap.Strings("columns", columnNames(row)))
		return
	}
	safe := redactor.Redact(schema, table, row)
	fields := make([]zap.Field, 0, len(safe)+3)
	fields = append(fields, zap.String("schema", schema), zap.String("table", table))
	for c, v := range safe {
		fields = append(fields, zap.Any("col."+c, v))
	}
	l.Log(level, msg, fields...)
}

// columnNames 返回排序后的列名清单（nil 行返回空切片）。只暴露列名，不暴露值。
func columnNames(row map[string]any) []string {
	if len(row) == 0 {
		return []string{}
	}
	names := make([]string, 0, len(row))
	for c := range row {
		names = append(names, c)
	}
	sort.Strings(names)
	return names
}
