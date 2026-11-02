// Package elector 实现 leader 选举（03 §4）：
//
//   - NewSession 失败 → 指数退避重试，goroutine 永不退出；Campaign 失败同样
//     退避；
//   - term = election key 的 revision；
//   - leader 指针 {prefix}/leader 挂当选 lease，lease 死亡自动消失；
//   - leader 在位复核（leader_check_interval_ms）：lease 存活 + election key
//     仍归属本节点，失败立即回调 OnLost 停管线（03 §4.2-1 防脑裂）；
//   - follower 持续 watch leader 指针（只观测一次会漏掉失主/换主）；follower
//     不启动管线；
//   - 状态机由选举 goroutine 独占驱动 + atomic role，无共享 chan（杜绝跨
//     goroutine 乱序触发状态迁移）；停机显式 Resign（ctx 超时保护，确保
//     失联时也能释放 leader 身份）。
//
// 防脑裂语义（03 §4.2，文档化承诺）：分区旧 leader 的「投递重叠」最长 TTL，
// 由下游幂等消化；位点/全量写的 lease 门槛（position 包）阻断旧 leader 持久化。
package elector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"

	"gocdc/internal/etcdx"
)

// Role 节点角色。
const (
	RoleFollower int32 = iota
	RoleLeader
)

// Options 选举参数。
type Options struct {
	Prefix              string // 集群 key 前缀 /gocdc/{cluster}
	NodeID              string
	AdvertiseAddr       string
	LeaseTTL            int64         // 选举 lease 秒数（03 §3.2 必填 ≥2，建议 10）
	LeaderCheckInterval time.Duration // 在位复核周期；0 = 2s
	Log                 *zap.Logger

	// OnElected 当选回调（装配层在此启动管线 + 位点写门槛切换）。
	// 回调阻塞会阻塞选举循环——装配层须转 goroutine。
	OnElected func(info Elected)
	// OnLost 失去 leader（lease 过期/复核失败/主动 Resign）；装配层停管线。
	OnLost func(reason string)
}

// Elected 当选信息（term = election key revision；LeaderKey = leader 指针 key，
// 装配层把它与 lease 一起设为位点写门槛）。
type Elected struct {
	Term      int64
	LeaseID   clientv3.LeaseID
	LeaderKey string
}

// Elector 选举器：Run 阻塞驱动状态机（ctx 取消 = 停机 Resign 后返回）。
type Elector struct {
	opts Options
	cli  *clientv3.Client
	log  *zap.Logger

	role     atomic.Int32
	term     atomic.Int64
	leader   atomic.Pointer[string] // 当前 leader 指针值（持续 watch，follower 展示用）
	sess     *concurrency.Session
	election *concurrency.Election

	resignCh chan struct{} // 请求主动让位（停机）
	mu       sync.Mutex
}

// New 构造。
func New(cli *clientv3.Client, opts Options) *Elector {
	if opts.LeaderCheckInterval <= 0 {
		opts.LeaderCheckInterval = 2 * time.Second
	}
	if opts.Log == nil {
		opts.Log = zap.NewNop()
	}
	return &Elector{opts: opts, cli: cli, log: opts.Log, resignCh: make(chan struct{}, 1)}
}

// Role 当前角色（原子读，Web/指标用）。
func (e *Elector) Role() int32 { return e.role.Load() }

// Term 当前 term（0 = 从未当选）。
func (e *Elector) Term() int64 { return e.term.Load() }

// leaderKey / electionPrefix key 派生（全部从配置前缀派生，禁硬编码）。
func (e *Elector) electionPrefix() string { return e.opts.Prefix + "/election" }
func (e *Elector) leaderKey() string      { return e.opts.Prefix + "/leader" }

// RequestResign 请求主动让位（非阻塞；停机序列调用）。
func (e *Elector) RequestResign() {
	select {
	case e.resignCh <- struct{}{}:
	default:
	}
}

// LeaderPtr 当前 leader 指针值（"" = 无主；持续 watch 维护，03 §4.1 follower 侧）。
func (e *Elector) LeaderPtr() string {
	if p := e.leader.Load(); p != nil {
		return *p
	}
	return ""
}

// watchLeaderPtr 持续 watch {prefix}/leader：值变化 → 更新；key 删除 → 无主
// （follower 必须持续观测，不能只读一次）。
func (e *Elector) watchLeaderPtr(ctx context.Context) {
	refresh := func() {
		resp, err := e.cli.Get(ctx, e.leaderKey())
		if err == nil && len(resp.Kvs) > 0 {
			v := string(resp.Kvs[0].Value)
			e.leader.Store(&v)
		} else if err == nil {
			e.leader.Store(nil)
		}
	}
	refresh()
	wch := e.cli.Watch(ctx, e.leaderKey())
	for {
		select {
		case <-ctx.Done():
			return
		case wresp, ok := <-wch:
			if !ok {
				return
			}
			for range wresp.Events {
				refresh()
			}
		}
	}
}

// Run 选举主循环（永不因 etcd 瞬态错误退出；ctx 取消 → Resign 后返回）。
func (e *Elector) Run(ctx context.Context) error {
	go e.watchLeaderPtr(ctx)
	backoff := 100 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			e.beFollower("shutdown")
			rctx, rcancel := context.WithTimeout(context.Background(), 3*time.Second)
			err := e.resignIfAny(rctx)
			rcancel()
			return err
		}

		// ① session：创建失败退避重试，绝不退出（03 §4.1）
		sess, err := etcdx.NewSession(ctx, e.cli, e.opts.LeaseTTL)
		if err != nil {
			e.log.Warn("创建选举 session 失败，退避重试", zap.Error(err), zap.Duration("backoff", backoff))
			if !sleepCtx(ctx, backoff) {
				continue
			}
			backoff = nextBackoff(backoff)
			continue
		}

		// ② campaign
		election := concurrency.NewElection(sess, e.electionPrefix())
		nodeVal := nodeValue(e.opts.NodeID, e.opts.AdvertiseAddr)
		cctx, cancelCampaign := context.WithCancel(ctx)
		err = election.Campaign(cctx, nodeVal)
		cancelCampaign()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				e.beFollower("shutdown")
				rctx, rcancel := context.WithTimeout(context.Background(), 3*time.Second)
				rerr := e.resignIfAny(rctx)
				rcancel()
				return rerr
			}
			e.log.Warn("campaign 失败，退避重试", zap.Error(err), zap.Duration("backoff", backoff))
			_ = sess.Close()
			if !sleepCtx(ctx, backoff) {
				continue
			}
			backoff = nextBackoff(backoff)
			continue
		}
		backoff = 100 * time.Millisecond // 成功重置

		// ③ 当选：term = election key revision；leader 指针挂本 lease（自动清理）
		term := int64(0)
		if resp, gerr := e.cli.Get(ctx, string(election.Key())); gerr == nil && len(resp.Kvs) > 0 {
			term = resp.Kvs[0].ModRevision
		}
		e.mu.Lock()
		e.sess, e.election = sess, election
		e.mu.Unlock()
		e.role.Store(RoleLeader)
		e.term.Store(term)
		if _, perr := e.cli.Put(ctx, e.leaderKey(), e.opts.NodeID, clientv3.WithLease(sess.Lease())); perr != nil {
			e.log.Warn("leader 指针写入失败（不影响当选）", zap.Error(perr))
		}
		e.log.Info("当选 leader",
			zap.String("node", e.opts.NodeID), zap.Int64("term", term),
			zap.String("election_key", string(election.Key())))
		if e.opts.OnElected != nil {
			e.opts.OnElected(Elected{Term: term, LeaseID: sess.Lease(), LeaderKey: e.leaderKey()})
		}

		// ④ leader 在位：watch lease 死亡 / 主动 Resign / 复核失败
		lost := e.leadUntilLost(ctx, sess, election)
		e.beFollower(lost)
		e.mu.Lock()
		e.election = nil // 失位即弃引用（防停机时对过期 election 误 Resign）
		e.mu.Unlock()
		if e.opts.OnLost != nil {
			e.opts.OnLost(lost)
		}
		// 让位（lease 死亡场景 resign 必失败，忽略错误继续重选）
		rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		_ = election.Resign(rctx)
		rcancel()
		_ = sess.Close()
		// 复核失败/主动让位 → 立即参与重选；lease 死亡 → 上层循环重建 session
	}
}

// leadUntilLost leader 在位监控（03 §4.2-1）：lease Done / 主动 Resign /
// 周期复核（election key 归属 + leader 指针仍为本节点）。返回失去原因。
func (e *Elector) leadUntilLost(ctx context.Context, sess *concurrency.Session, election *concurrency.Election) string {
	ticker := time.NewTicker(e.opts.LeaderCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return "shutdown"
		case <-sess.Done():
			return "lease_expired"
		case <-e.resignCh:
			return "resign_requested"
		case <-ticker.C:
			// 复核必须有界：etcd 分区/宕机时裸 cli.Get 会阻塞在 balancer 重试上，
			// 不设超时则 leader 长期不失位、管线照常投递（违反 03 §4.2-1
			// 「分区 → 复核失败 → TTL 内立即停管线」）。超时即视为复核失败 → OnLost。
			cctx, cancel := context.WithTimeout(ctx, e.checkTimeout())
			reason := e.verifyLeadership(cctx, sess.Lease(), string(election.Key()))
			cancel()
			if reason != "" {
				return reason
			}
		}
	}
}

// checkTimeout 单次在位复核的 etcd 调用超时：2×复核周期，且不超过 lease TTL
// 的一半（保证「分区 → TTL 内停管线」；下限 1s 防配置过小导致误判抖动）。
func (e *Elector) checkTimeout() time.Duration {
	t := 2 * e.opts.LeaderCheckInterval
	if ttl := time.Duration(e.opts.LeaseTTL) * time.Second; ttl > 0 && t > ttl/2 {
		t = ttl / 2
	}
	if t < time.Second {
		t = time.Second
	}
	return t
}

// verifyLeadership 在位复核：election key 仍存在且值=本节点（lease 失效 etcd
// 已删 key）；leader 指针 key 须仍指向本节点——缺失/易主时用当选 lease 重写
// （自愈：位点写门槛以该 key 的 lease 为锚，指针丢失 = 全集群位点写被拒瘫痪）。
// 失败返回原因，成功返回空。
func (e *Elector) verifyLeadership(ctx context.Context, leaseID clientv3.LeaseID, electionKey string) string {
	resp, err := e.cli.Get(ctx, electionKey)
	if err != nil {
		return "etcd_error: " + err.Error()
	}
	if len(resp.Kvs) == 0 {
		return "election_key_gone"
	}
	if nodeIDOf(string(resp.Kvs[0].Value)) != e.opts.NodeID {
		return "election_key_taken"
	}

	// leader 指针复核 + 自愈重写（挂同一当选 lease：lease 死亡自动消失）
	lp, perr := e.cli.Get(ctx, e.leaderKey())
	if perr != nil {
		e.log.Warn("leader 指针复核读取失败（下一周期重试）", zap.Error(perr))
		return "" // etcd 瞬态：不据此失位（election key 归属仍成立）
	}
	if len(lp.Kvs) == 0 || string(lp.Kvs[0].Value) != e.opts.NodeID {
		if _, perr := e.cli.Put(ctx, e.leaderKey(), e.opts.NodeID, clientv3.WithLease(leaseID)); perr != nil {
			e.log.Warn("leader 指针自愈重写失败（下一周期重试）", zap.Error(perr))
		} else {
			e.log.Warn("leader 指针缺失/易主，已重写自愈", zap.String("key", e.leaderKey()))
		}
	}
	return ""
}

// nodeIDOf 从 election key 值（nodeValue JSON）解析 node_id。
// 解析失败返回原文（与 NodeID 全等比较而非子串包含：节点 ID 互为前缀时
// Contains 会误判归属）。
func nodeIDOf(v string) string {
	var nv struct {
		NodeID string `json:"node_id"`
	}
	if err := json.Unmarshal([]byte(v), &nv); err == nil && nv.NodeID != "" {
		return nv.NodeID
	}
	return v
}

// beFollower 重置角色（幂等）。
func (e *Elector) beFollower(reason string) {
	if e.role.Swap(RoleFollower) == RoleLeader {
		e.log.Info("失去 leader", zap.String("reason", reason))
	}
}

// resignIfAny 停机让位：仅当本节点在任（或曾当选且 election 尚存）才 Resign +
// 删 leader 指针（03 §4.1）。follower 绝不能动 leader 指针——位点写门槛以该
// key 的 lease 为锚（position 包），follower 删除它 = 在任 leader 的全部位点写
// 被门槛拒绝 → 熔断停管线，且无任何节点会重写该 key（P0：滚动重启 follower
// 即瘫集群）。
func (e *Elector) resignIfAny(ctx context.Context) error {
	e.mu.Lock()
	wasLeader := e.role.Load() == RoleLeader
	election := e.election
	e.election = nil
	e.mu.Unlock()
	if election == nil {
		return ctx.Err()
	}
	if wasLeader {
		if err := election.Resign(ctx); err != nil {
			e.log.Warn("Resign 失败（lease 可能已亡）", zap.Error(err))
		}
		_, _ = e.cli.Delete(ctx, e.leaderKey())
	}
	return ctx.Err()
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > 10*time.Second {
		d = 10 * time.Second
	}
	return d
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func nodeValue(nodeID, addr string) string {
	return fmt.Sprintf(`{"node_id":%q,"advertise_addr":%q}`, nodeID, addr)
}
