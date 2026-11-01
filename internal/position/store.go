// Package position 实现位点的 etcd 存储与保存节流（03 §5）：
//   - Store：位点 key 读写；写侧带 lease 门槛事务——Compare LeaseID(gateKey)
//     必须等于本进程 lease（选主后即 leader 的 election key；单节点部署时为节点
//     注册 key），lease 失效/失主时写入被拒（03 §4-2：新 leader 位点不被旧
//     leader 覆盖，防分区双写）。
//   - Throttler：XID 路径节流；rotate/DDL/停机/全量交接为强制保存，不走节流
//     （03 §5.2 触发清单，强制路径由 Persister 直接调 Store.Save）。
//
// Position 模型见 internal/model（与 03 §5.1 JSON 结构一致）。
package position

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"gocdc/internal/etcdx"
	"gocdc/internal/model"
)

// ErrGateRejected lease 门槛未通过：gateKey 不存在或附着 lease 已易主——
// 当前进程无权推进位点（继续消费只会放大重放窗口，Persister 据此重试/熔断）。
var ErrGateRejected = errors.New("position: lease 门槛未通过，位点写入被拒")

const positionVersion = 1

// Store 位点 etcd 存储。
type Store struct {
	mu      sync.RWMutex
	kv      *etcdx.KV
	key     string           // 位点 key（{prefix}/position 或 position_key 覆盖）
	gateKey string           // lease 门槛锚点 key（= leader 指针 key）
	leaseID clientv3.LeaseID // 本任期（当选 lease）；0 = 无门槛（写全拒）
	now     func() time.Time
}

// NewStore 构造。gateKey/leaseID 由装配层注入；初始无门槛（leaseID=0）= 写被拒，
// 直到装配层在当选后 SetGate（写门槛全覆盖：follower/无主绝不写位点）。
func NewStore(kv *etcdx.KV, key, gateKey string, leaseID clientv3.LeaseID) *Store {
	return &Store{kv: kv, key: key, gateKey: gateKey, leaseID: leaseID, now: time.Now}
}

// SetGate 重设写门槛：当选后指向 leader 指针 key + 当选 lease——
// {prefix}/leader 挂当选 lease，lease 死亡自动消失，新 leader 换新 lease，
// 旧/无主节点的 Compare(LeaseValue)==本lease 恒不成立 → 位点写被拒。
// 并发安全（与 Save 竞争读 gate 快照）。
func (s *Store) SetGate(gateKey string, leaseID clientv3.LeaseID) {
	s.mu.Lock()
	s.gateKey, s.leaseID = gateKey, leaseID
	s.mu.Unlock()
}

// Load 读位点。key 不存在返回 (零值, false, nil)——首启无位点属正常态，
// 由装配层决定起点（SHOW MASTER STATUS / 全量快照 P）。
func (s *Store) Load(ctx context.Context) (model.Position, bool, error) {
	resp, err := s.kv.Get(ctx, s.key)
	if err != nil {
		return model.Position{}, false, fmt.Errorf("position: 读取 %s: %w", s.key, err)
	}
	if len(resp.Kvs) == 0 {
		return model.Position{}, false, nil
	}
	var p model.Position
	if err := json.Unmarshal(resp.Kvs[0].Value, &p); err != nil {
		return model.Position{}, false, fmt.Errorf("position: 位点 %s 解析失败: %w", s.key, err)
	}
	return p, true, nil
}

// Save 写位点（03 §5.2）：etcd Put 本身线性一致，无需 CAS version；
// 门槛 = 事务 Compare LeaseID(gateKey)==本 lease，失败返回 ErrGateRejected。
func (s *Store) Save(ctx context.Context, p model.Position) error {
	p.V = positionVersion
	p.UpdatedAt = s.now()
	buf, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("position: 序列化失败: %w", err)
	}
	s.mu.RLock()
	gateKey, leaseID := s.gateKey, s.leaseID
	s.mu.RUnlock()
	if leaseID == 0 {
		return ErrGateRejected // 无门槛期（未当选）：绝不写位点
	}
	resp, err := s.kv.Txn(ctx, func(t clientv3.Txn) clientv3.Txn {
		return t.If(clientv3.Compare(clientv3.LeaseValue(gateKey), "=", int64(leaseID))).
			Then(clientv3.OpPut(s.key, string(buf)))
	})
	if err != nil {
		return fmt.Errorf("position: 写入 %s: %w", s.key, err)
	}
	if !resp.Succeeded {
		return ErrGateRejected
	}
	return nil
}
