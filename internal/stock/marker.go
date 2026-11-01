// 每表进度标记存储（06 全文）：running/done/failed 状态 + last_pk 断点 +
// 全局 stock_position（P 先行持久化）；写入经 lease 门槛事务。
//
// 会话约束（06 §4，正确性关键）：last_pk 断点只在同一快照会话内有效——
// session_id 不匹配 → 断点作废整表重扫（防错序污染）。
// 包级概述见 doc.go。

package stock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"

	"gocdc/internal/etcdx"
	"gocdc/internal/model"
)

// 表状态。
const (
	StatusRunning = "running"
	StatusDone    = "done"
	StatusFailed  = "failed"
)

// Marker 每表进度标记（06 §4：etcd stock/{db}.{table}）。
type Marker struct {
	Status    string         `json:"status"`
	SessionID string         `json:"session_id"`
	SnapshotP model.Position `json:"snapshot_position"`
	LastPK    []string       `json:"last_pk"` // PK 值文本形态（按列类型还原绑定）
	ChunkSeq  int64          `json:"chunk_seq"`
	RowsDone  int64          `json:"rows_done"`
	Error     string         `json:"error,omitempty"`
}

// ErrGateRejected 标记写门槛未通过（03 §4.2-2：stock/* 写同样受 lease 门槛——
// 防分区旧 leader 继续推进/覆盖全量状态）。
var ErrGateRejected = errors.New("stock: 标记写门槛未通过")

// MarkerStore 进度标记存储（lease 门槛事务与 position.Store 同款语义）。
type MarkerStore struct {
	mu      sync.RWMutex
	kv      *etcdx.KV
	prefix  string // {cluster 前缀}/stock
	gateKey string
	leaseID clientv3.LeaseID
}

// NewMarkerStore 构造；初始无门槛（写被拒），当选后 SetGate。
func NewMarkerStore(kv *etcdx.KV, prefix string) *MarkerStore {
	return &MarkerStore{kv: kv, prefix: prefix}
}

// SetGate 重设写门槛（同 position.Store.SetGate；当选回调调用）。
func (m *MarkerStore) SetGate(gateKey string, leaseID clientv3.LeaseID) {
	m.mu.Lock()
	m.gateKey, m.leaseID = gateKey, leaseID
	m.mu.Unlock()
}

func (m *MarkerStore) key(db, table string) string {
	return m.prefix + "/" + strings.ToLower(db) + "." + strings.ToLower(table)
}

// GlobalPositionKey 全局 stock_position key（06 §4.1：增量启动位点 = max 所有快照对）。
func (m *MarkerStore) GlobalPositionKey() string { return m.prefix + "/_global_position" }

// Get 读单表标记；不存在返回 (nil, nil)。
func (m *MarkerStore) Get(ctx context.Context, db, table string) (*Marker, error) {
	resp, err := m.kv.Get(ctx, m.key(db, table))
	if err != nil {
		return nil, fmt.Errorf("stock: 读标记 %s.%s: %w", db, table, err)
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	var mk Marker
	if err := json.Unmarshal(resp.Kvs[0].Value, &mk); err != nil {
		return nil, fmt.Errorf("stock: 标记 %s.%s 解析失败: %w", db, table, err)
	}
	return &mk, nil
}

// Put 写标记（lease 门槛事务）。
func (m *MarkerStore) Put(ctx context.Context, db, table string, mk *Marker) error {
	buf, err := json.Marshal(mk)
	if err != nil {
		return err
	}
	m.mu.RLock()
	gateKey, leaseID := m.gateKey, m.leaseID
	m.mu.RUnlock()
	if leaseID == 0 {
		return ErrGateRejected
	}
	resp, err := m.kv.Txn(ctx, func(t clientv3.Txn) clientv3.Txn {
		return t.If(clientv3.Compare(clientv3.LeaseValue(gateKey), "=", int64(leaseID))).
			Then(clientv3.OpPut(m.key(db, table), string(buf)))
	})
	if err != nil {
		return fmt.Errorf("stock: 写标记 %s.%s: %w", db, table, err)
	}
	if !resp.Succeeded {
		return ErrGateRejected
	}
	return nil
}

// PutGlobalPosition 持久化全局 stock_position（P 先行：先于全量扫描，06 §2-4）。
func (m *MarkerStore) PutGlobalPosition(ctx context.Context, p model.Position) error {
	buf, err := json.Marshal(p)
	if err != nil {
		return err
	}
	m.mu.RLock()
	gateKey, leaseID := m.gateKey, m.leaseID
	m.mu.RUnlock()
	if leaseID == 0 {
		return ErrGateRejected
	}
	resp, err := m.kv.Txn(ctx, func(t clientv3.Txn) clientv3.Txn {
		return t.If(clientv3.Compare(clientv3.LeaseValue(gateKey), "=", int64(leaseID))).
			Then(clientv3.OpPut(m.GlobalPositionKey(), string(buf)))
	})
	if err != nil {
		return fmt.Errorf("stock: 写全局位点: %w", err)
	}
	if !resp.Succeeded {
		return ErrGateRejected
	}
	return nil
}

// GlobalPosition 读全局 stock_position（不存在返回 false）。
func (m *MarkerStore) GlobalPosition(ctx context.Context) (model.Position, bool, error) {
	resp, err := m.kv.Get(ctx, m.GlobalPositionKey())
	if err != nil {
		return model.Position{}, false, err
	}
	if len(resp.Kvs) == 0 {
		return model.Position{}, false, nil
	}
	var p model.Position
	if err := json.Unmarshal(resp.Kvs[0].Value, &p); err != nil {
		return model.Position{}, false, err
	}
	return p, true, nil
}

// ResetTable 清除表标记（restock；06 §5：清标记 → 重跑全量）。
func (m *MarkerStore) ResetTable(ctx context.Context, db, table string) error {
	_, err := m.kv.Delete(ctx, m.key(db, table))
	return err
}

// GC 删除白名单外的标记——组形态下这些表不会再被 runStock 扫描（只遍历规则
// 表），其标记永远不会再被读取，留着即 etcd 垃圾。done 标记本身保留
// （「重启不重跑」语义依赖，见 lifecycle.runStock）。keep：小写 "db.table"
// → 是否保留；全局位点 key（_global_position）恒保留。返回删除的 key 数。
func (m *MarkerStore) GC(ctx context.Context, keep func(dbTable string) bool) (int, error) {
	resp, err := m.kv.Get(ctx, m.prefix+"/", clientv3.WithPrefix())
	if err != nil {
		return 0, fmt.Errorf("stock: GC 列举标记失败: %w", err)
	}
	removed := 0
	for _, k := range resp.Kvs {
		// 先按原始 key 剥前缀、再小写——前缀可能含大写（如测试的 markerGC），
		// 先 ToLower 会让 TrimPrefix 落空、suffix 变全路径，全局位点也会被误删
		suffix := strings.ToLower(strings.TrimPrefix(string(k.Key), m.prefix+"/"))
		if suffix == "" || suffix == "_global_position" || keep(suffix) {
			continue
		}
		if _, err := m.kv.Delete(ctx, string(k.Key)); err != nil {
			return removed, fmt.Errorf("stock: GC 删除 %s: %w", string(k.Key), err)
		}
		removed++
	}
	return removed, nil
}

// decodePK 按 schema 列类型把 last_pk 文本还原为绑定值（文本协议回读语义）。
func decodePK(cols []model.Column, vals []string) ([]any, error) {
	if len(cols) != len(vals) {
		return nil, fmt.Errorf("stock: last_pk 列数不匹配 %d/%d", len(vals), len(cols))
	}
	out := make([]any, len(vals))
	for i, s := range vals {
		switch cols[i].Type {
		case model.TypeTinyint, model.TypeSmallint, model.TypeMediumint, model.TypeInt, model.TypeBigint:
			if cols[i].Unsigned {
				u, err := strconv.ParseUint(s, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("stock: last_pk[%d]=%q 还原无符号整型失败: %w", i, s, err)
				}
				out[i] = u
			} else {
				n, err := strconv.ParseInt(s, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("stock: last_pk[%d]=%q 还原有符号整型失败: %w", i, s, err)
				}
				out[i] = n
			}
		case model.TypeYear:
			n, _ := strconv.ParseInt(s, 10, 64)
			out[i] = n
		default:
			out[i] = s
		}
	}
	return out, nil
}

// encodePK 主键值 → 文本形态（存标记）。
func encodePK(vals []any) []string {
	out := make([]string, len(vals))
	for i, v := range vals {
		switch x := v.(type) {
		case nil:
			out[i] = "null"
		case string:
			out[i] = x
		case []byte:
			out[i] = string(x)
		case int64:
			out[i] = strconv.FormatInt(x, 10)
		case uint64:
			out[i] = strconv.FormatUint(x, 10)
		case float64:
			out[i] = strconv.FormatFloat(x, 'f', -1, 64)
		default:
			out[i] = fmt.Sprintf("%v", x)
		}
	}
	return out
}
