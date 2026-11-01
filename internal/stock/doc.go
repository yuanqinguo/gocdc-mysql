// Package stock 实现全量同步：一致性快照会话（session_id 约束）、
// 锁降级矩阵探测、PK 游标分块扫描（联合主键行值构造器）、表级完成标记
// （lease 门槛写入）、全量-增量交接协议（增量起点 = max(所有快照对)）与 restock。
//
// 权威设计：docs/arch/06-full-sync-handover.md（恢复协议 §4.1）。
package stock
