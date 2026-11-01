// Package schema 维护源表结构缓存（model.TableMeta）：information_schema 加载、
// 版本化原子替换（不可变快照整体替换）、PK/注释/charset 提取、DDL 触发刷新。
//
// 权威设计：docs/arch/08-ddl-strategy.md §3（列名对齐，禁止按列数/位置对齐）。
package schema
