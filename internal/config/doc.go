// Package config 提供全量配置：结构体模型（字段与 03 §3.2 schema 一一对应）、
// 本地文件/etcd 双通道加载、${ENV} 占位解析、cluster 先后裁决（03 §3.1）与
// 严格交叉校验（校验失败输出全部错误清单后退出）。
//
// 权威设计：docs/arch/03-etcd-ha-config.md §3。
// MQ 客户端：Kafka 用 franz-go，RocketMQ 用 v5 gRPC 客户端。
//
// ── 30 秒上手 ─────────────────────────────────────────────────────────
// 完整样例与逐字段说明见本目录 README.md。配置正文为 YAML（唯一格式）。
// v1 单源写法（顶层 source/targets/rules）加载后自动包装为
// sources[default]（多源 PRD 03 §4）；v2 多源写法见 README §4.5。
// 最小骨架（v1 形态，可通过全部校验）：
//
//	cluster: dev
//	source:
//	  addr: "10.0.0.1:3306"
//	  user: repl
//	  password: ${MYSQL_PWD}
//	  charset: utf8mb4
//	  flavor: mysql
//	  server_id: 123456
//	  timezone: Asia/Shanghai
//	  exclude_schemas: [mysql, sys, information_schema, performance_schema]
//	ha: { lease_ttl_seconds: 10 }
//	pipeline: { workers: 4, worker_queue_size: 8192, batch_size: 500 }
//	targets:
//	  mysql:
//	    enabled: true
//	    addr: "10.0.1.1:3306"
//	    user: writer
//	    password: ${MYSQL_PWD}
//	    charset: utf8mb4
//	    max_open_conns: 20
//	    tx_chunk_rows: 500
//	rules:
//	  - schema: edu.mall.admin
//	    sinks: { mysql: true }
//	observability: { metrics_addr: ":9595", log: { level: info } }
package config
