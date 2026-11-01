// Package mysql 实现 MySQL 直写 sink：同名同表、列一一对应（不支持字段映射）、
// 批量分组 upsert / IN 删除 / PK 变更 delete+upsert、游程分组保序（05 §5.3）、
// tx_chunk_rows 切块事务、目标表结构核对启动检查。
//
// 权威设计：docs/arch/05-sinks-and-message.md §5。
package mysql
