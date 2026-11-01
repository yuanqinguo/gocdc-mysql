package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"gocdc/internal/etcdx"
)

// DDLGate 审批 gate 契约（08 §5；etcd 实现为 *EtcdDDLGate，测试可注入 fake）：
// pause 策略的 DDL 写入 etcd events/ddl 挂起记录，阻塞等待人工决策
// （Web/CLI 更新状态）。断点续跑：重启重放到同一 DDL 时 Settled 命中既有
// 决策 → 幂等执行。
//
// key 契约：{prefix}/events/ddl/{db.table}.{file}.{pos}
// value：DDLRecord JSON。决策 = 更新 status 字段（approve | skip）。
type DDLGate interface {
	Record(ctx context.Context, rec DDLRecord) error
	Settled(ctx context.Context, id string) (decision string, ok bool, err error)
	WaitDecision(ctx context.Context, id string) (decision string, err error)
	Settle(ctx context.Context, id, decision, note string) error
}

// DDLLister 挂起记录查询（Web 列表用；EtcdDDLGate 实现）。
type DDLLister interface {
	PendingByTable(ctx context.Context, schemaName, table string) ([]DDLRecord, error)
}

// EtcdDDLGate DDLGate 的 etcd 实现。
type EtcdDDLGate struct {
	kv     *etcdx.KV
	prefix string // {cluster 前缀}/events/ddl
}

// NewDDLGate 构造 etcd 实现。
func NewDDLGate(kv *etcdx.KV, prefix string) *EtcdDDLGate {
	return &EtcdDDLGate{kv: kv, prefix: prefix}
}

// DDLRecord 挂起/已决的 DDL 事件（08 §5：全部 DDL 事件含决策写 etcd + 审计日志）。
type DDLRecord struct {
	ID        string    `json:"id"` // {db.table}.{file}.{pos}（重放稳定）
	Schema    string    `json:"schema"`
	Table     string    `json:"table"`
	SQL       string    `json:"sql"`
	Position  string    `json:"position"` // file:pos
	Policy    string    `json:"policy"`   // pause | auto-degraded
	Status    string    `json:"status"`   // pending | approve | skip | synced
	CreatedAt time.Time `json:"created_at"`
	DecidedAt time.Time `json:"decided_at,omitempty"`
	Note      string    `json:"note,omitempty"`
}

// DDL 决策值。
const (
	DecisionPending = "pending"
	DecisionApprove = "approve"
	DecisionSkip    = "skip"
	DecisionSynced  = "synced" // auto 白名单已自动执行
)

func (g *EtcdDDLGate) key(id string) string {
	return g.prefix + "/" + strings.ReplaceAll(id, "`", "")
}

// Record 写入挂起记录（已决记录不覆盖——断点续跑幂等：重启重放同一 DDL
// 时保留 approve/skip/synced 决策，绝不回退成 pending）。
func (g *EtcdDDLGate) Record(ctx context.Context, rec DDLRecord) error {
	if _, settled, err := g.Settled(ctx, rec.ID); err == nil && settled {
		return nil // 已决：保留原决策
	} else if err != nil {
		return fmt.Errorf("ddlgate: 查既有决策 %s: %w", rec.ID, err)
	}
	rec.CreatedAt = time.Now()
	if rec.Status == "" {
		rec.Status = DecisionPending
	}
	buf, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := g.kv.Put(ctx, g.key(rec.ID), string(buf)); err != nil {
		return fmt.Errorf("ddlgate: 写记录 %s: %w", rec.ID, err)
	}
	return nil
}

// Settled 既有决策（断点续跑幂等）：status ∈ {approve, skip, synced} 返回之。
func (g *EtcdDDLGate) Settled(ctx context.Context, id string) (string, bool, error) {
	resp, err := g.kv.Get(ctx, g.key(id))
	if err != nil {
		return "", false, err
	}
	if len(resp.Kvs) == 0 {
		return "", false, nil
	}
	var rec DDLRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
		return "", false, err
	}
	if rec.Status == DecisionApprove || rec.Status == DecisionSkip || rec.Status == DecisionSynced {
		return rec.Status, true, nil
	}
	return "", false, nil
}

// Settle 更新决策（Web/CLI 调用的底层动作，admin API 暴露）。
func (g *EtcdDDLGate) Settle(ctx context.Context, id, decision, note string) error {
	resp, err := g.kv.Get(ctx, g.key(id))
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return fmt.Errorf("ddlgate: 记录 %s 不存在", id)
	}
	var rec DDLRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
		return err
	}
	rec.Status = decision
	rec.DecidedAt = time.Now()
	rec.Note = note
	buf, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = g.kv.Put(ctx, g.key(id), string(buf))
	return err
}

// PendingByTable 列出未决（pending）的 DDL 记录（Web 挂起列表/测试用）。
// schema 与 table 均空 = 全部挂起记录（RUNBOOK §5 排障入口不带参数裸调）。
func (g *EtcdDDLGate) PendingByTable(ctx context.Context, schemaName, table string) ([]DDLRecord, error) {
	resp, err := g.kv.Get(ctx, g.prefix+"/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	all := schemaName == "" && table == ""
	suffix := strings.ToLower(schemaName) + "." + strings.ToLower(table) + "."
	var out []DDLRecord
	for _, kvs := range resp.Kvs {
		var rec DDLRecord
		if err := json.Unmarshal(kvs.Value, &rec); err != nil {
			continue
		}
		if rec.Status != DecisionPending {
			continue
		}
		// 记录 ID 即 "{db}.{table}.{file}.{pos}"（不含 etcd 前缀）
		if all || strings.HasPrefix(rec.ID, suffix) {
			out = append(out, rec)
		}
	}
	return out, nil
}

// WaitDecision 阻塞轮询决策（2s 周期；ctx 取消返回）。pause 的人工等待期，
// 该 worker 队列停摆 → 水位洞 → 位点冻结（03/08 语义：等待人工介入）。
func (g *EtcdDDLGate) WaitDecision(ctx context.Context, id string) (string, error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		dec, ok, err := g.Settled(ctx, id)
		if err != nil {
			return "", err
		}
		if ok {
			return dec, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

// DDLGCRetention 已决记录保留期：超过即删（每条 DDL 一个 key，永久保留会使
// etcd 数据无界累积）。断点续跑幂等只依赖「可能被重放」的窗口，7 天远超任何
// 重放窗口；pending 记录永不清理（等待人工决策的语义不能被后台任务吞掉）。
const DDLGCRetention = 7 * 24 * time.Hour

// GC 删除已决（approve/skip/synced）且 DecidedAt 早于 olderThan 的记录；
// pending 永不删。返回删除的记录数（leader 侧低频任务调用，见 lifecycle.gcLoop）。
func (g *EtcdDDLGate) GC(ctx context.Context, olderThan time.Duration) (int, error) {
	resp, err := g.kv.Get(ctx, g.prefix+"/", clientv3.WithPrefix())
	if err != nil {
		return 0, fmt.Errorf("ddlgate: GC 列举失败: %w", err)
	}
	removed := 0
	cutoff := time.Now().Add(-olderThan)
	for _, kvs := range resp.Kvs {
		var rec DDLRecord
		if err := json.Unmarshal(kvs.Value, &rec); err != nil {
			continue
		}
		if rec.Status != DecisionApprove && rec.Status != DecisionSkip && rec.Status != DecisionSynced {
			continue
		}
		if rec.DecidedAt.IsZero() || rec.DecidedAt.After(cutoff) {
			continue
		}
		if _, err := g.kv.Delete(ctx, string(kvs.Key)); err != nil {
			return removed, fmt.Errorf("ddlgate: GC 删除 %s: %w", string(kvs.Key), err)
		}
		removed++
	}
	return removed, nil
}

// DDLRecordID 生成重放稳定的记录 ID（08 §5 幂等：同 DDL 事件重放同 ID）。
func DDLRecordID(schema, table, file string, pos uint32) string {
	return fmt.Sprintf("%s.%s.%s.%d", strings.ToLower(schema), strings.ToLower(table), file, pos)
}
