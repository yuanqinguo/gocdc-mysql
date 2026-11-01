// Package etcdx 封装 etcd 客户端治理：多 endpoint/TLS/auth 构造、KV 读写重试退避、
// lease session 包装与节点注册（带 lease 的临时 key）。
//
// 唯一 import 路径 go.etcd.io/etcd/client/v3；必须用 client.KV（内置 balancer/
// failover），禁用 clientv3.NewKV。
//
// 版本锁定：client v3.6.8——v3.6.10+ 要求 Go ≥1.25，与 Go 1.24 冲突；
// 服务端兼容 v3.5+。
// 权威设计：docs/arch/03-etcd-ha-config.md §5.3。
package etcdx
