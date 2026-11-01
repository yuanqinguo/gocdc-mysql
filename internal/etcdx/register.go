package etcdx

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// NodeInfo 节点注册值（03 §2 {prefix}/nodes/{node_id}，字段名与 schema 对应）。
type NodeInfo struct {
	NodeID        string    `json:"node_id"`
	AdvertiseAddr string    `json:"advertise_addr"`
	Version       string    `json:"version"`
	StartedAt     time.Time `json:"started_at"`
	Role          string    `json:"role"` // leader | follower
}

// Registrar 节点注册：带 lease 的临时 key，lease 过期 etcd 自动删除（03 §2），
// 供 Web 展示集群成员与角色。key 全部从配置的 prefix 派生，禁止硬编码。
type Registrar struct {
	kv     *KV
	prefix string // 集群前缀 /gocdc/{cluster}
	info   NodeInfo
}

// NewRegistrar 构造节点注册器。prefix 为集群 key 前缀（如 /gocdc/edu-merge-prod）。
func NewRegistrar(kv *KV, prefix string, info NodeInfo) *Registrar {
	if info.StartedAt.IsZero() {
		info.StartedAt = time.Now()
	}
	return &Registrar{kv: kv, prefix: prefix, info: info}
}

// Key 返回注册 key 路径 {prefix}/nodes/{node_id}。
func (r *Registrar) Key() string {
	return r.prefix + "/nodes/" + r.info.NodeID
}

// Register 以 session 的 lease 写入注册 key（lease 过期自动删除）。
func (r *Registrar) Register(ctx context.Context, sess *concurrency.Session) error {
	val, err := json.Marshal(r.info)
	if err != nil {
		return fmt.Errorf("序列化节点信息: %w", err)
	}
	if _, err := r.kv.Put(ctx, r.Key(), string(val), clientv3.WithLease(sess.Lease())); err != nil {
		return fmt.Errorf("写入节点注册 %s: %w", r.Key(), err)
	}
	return nil
}

// Update 更新节点值（如角色切换 leader|follower），沿用同一 lease。
func (r *Registrar) Update(ctx context.Context, sess *concurrency.Session, role string) error {
	r.info.Role = role
	return r.Register(ctx, sess)
}

// Deregister 主动注销（优雅停机调用）；key 随后也可由 lease 过期兜底删除。
func (r *Registrar) Deregister(ctx context.Context) error {
	if _, err := r.kv.Delete(ctx, r.Key()); err != nil {
		return fmt.Errorf("删除节点注册 %s: %w", r.Key(), err)
	}
	return nil
}
