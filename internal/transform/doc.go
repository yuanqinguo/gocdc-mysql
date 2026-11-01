// Package transform 实现 binlog/全量统一的值类型转换（04 §7 全类型转换规格）、
// exclude/mask 列处理（10 文档算法）与统一 Event 构建（05 §2.1）。
//
// 红线：主键值不脱敏不排除；转换失败不允许静默变 NULL/0，按
// on_convert_error（null|zero|fail_event）显式处置；禁止 decimal→float64、
// JSON 反序列化成 map 绑参。
package transform
