// 错误定义。列注释/charset 元数据在此加载（DDL 白名单同步依赖）；列名对齐
// 位点语义由 source 层负责。包概述见 doc.go。

package schema

import "errors"

var (
	// ErrTableNotFound 表不存在（源库被 DROP 或库名/表名写错）。
	ErrTableNotFound = errors.New("schema: 表不存在")
	// ErrEmptySchema / ErrEmptyTable 参数缺失。
	ErrEmptySchema = errors.New("schema: 库名为空")
	ErrEmptyTable  = errors.New("schema: 表名为空")
)
