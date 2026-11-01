// Package source 封装 binlog 拉取：go-mysql-org/go-mysql BinlogSyncer
// （server_id/heartbeat/read_timeout/IncludeTableRegex/use_gtid）、断线监督重启循环、
// 事件解码为 pipeline.EventUnit（exclude_schemas 在此过滤）。
//
// 权威设计：docs/arch/04-pipeline-parallel.md；监督循环永不退出。
package source
