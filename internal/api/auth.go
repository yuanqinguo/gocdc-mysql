// auth.go：HTTP Basic 鉴权中间件与审计链路（审计事件写日志 + etcd 环形存储）；
// 包说明见 doc.go。

package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"gocdc/internal/etcdx"
	"gocdc/internal/logging"
)

// AuthConfig 鉴权参数（03 §3.2 observability.web）：单一用户名密码（HTTP
// Basic）——一套凭据全权限；危险操作另有 confirm 门禁与审计兜底。
type AuthConfig struct {
	Username         string // Basic 用户名（必填，除 auth_disabled）
	Password         string // Basic 密码（必填，除 auth_disabled；支持 ${ENV}）
	AuthDisabled     bool   // 显式声明关闭鉴权（内网裸奔，配置校验告警）
	DisableAuditEtcd bool   // 审计只写日志不写 etcd（单机开发）
}

// authMiddleware HTTP Basic 鉴权（单用户名密码）。恒时比较经 SHA-256 归一，
// 避免长度差泄漏；who = 用户名（审计）。
func authMiddleware(cfg AuthConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		if cfg.AuthDisabled {
			c.Set("who", "anonymous")
			c.Next()
			return
		}
		user, pass, ok := c.Request.BasicAuth()
		if !ok {
			abort401(c, "缺少 Authorization: Basic 凭据（用户名+密码）")
			return
		}
		if !equalCred(user, cfg.Username) || !equalCred(pass, cfg.Password) {
			abort401(c, "凭据无效")
			return
		}
		c.Set("who", user)
		c.Next()
	}
}

// equalCred 恒时比较：SHA-256 定长化，规避长度差时序泄漏。
func equalCred(a, b string) bool {
	ha := sha256.Sum256([]byte(a))
	hb := sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(ha[:], hb[:]) == 1
}

func abort401(c *gin.Context, msg string) { abort(c, 401, msg) }

// audit 审计事件（13 §3：who/what/when/结果 → 日志 + etcd events/audit 尾部
// 保留 100 条；etcd 写失败仅日志告警，不阻塞操作）。cluster 级操作用本函数，
// 源级操作用 auditSource（条目带 source 字段，多源 PRD 06 §2.5）。
func audit(kv AuditSink, c *gin.Context, what, detail string, result error) {
	auditSource(kv, c, "", what, detail, result)
}

// auditSource 源级审计（sid 空 = cluster 级操作，条目 source 显示 —）。
func auditSource(kv AuditSink, c *gin.Context, sid, what, detail string, result error) {
	who := "anonymous"
	if v, ok := c.Get("who"); ok {
		who = v.(string)
	}
	res := "ok"
	if result != nil {
		res = result.Error()
	}
	logging.L().Info("审计",
		logging.Event("audit"),
		zap.String("who", who), zap.String("what", what), zap.String("source", sid),
		zap.String("path", c.Request.URL.Path), zap.String("detail", detail),
		zap.String("result", res))
	if kv != nil {
		_ = kv.appendAuditSource(who, sid, what+" "+c.Request.URL.Path+" "+detail, res, time.Now())
	}
}

// auditList GET /api/v1/audit：最近审计事件（未装配 etcd 审计时空列表）。
func (h apiHandlers) auditList(c *gin.Context) {
	if auditOrNil(h.Audit) == nil {
		respondOK(c, []auditEntry{})
		return
	}
	list, err := h.Audit.List(c.Request.Context(), 100)
	if err != nil {
		respondErr(c, 500, err.Error())
		return
	}
	if list == nil {
		list = []auditEntry{}
	}
	respondOK(c, list)
}

// auditOrNil typed-nil → nil interface（避免 nil *AuditStore 触发 panic）。
func auditOrNil(a AuditSink) AuditSink {
	if a == nil {
		return nil
	}
	return a
}

// ── etcd 审计存储：单 key 环形保留 100 条（13 §3）──
//
// 形态：{prefix}/events/audit 一个 key 承载 JSON 数组（环形缓冲，尾部 100 条——
// 审计只留最近轨迹，防单 key 无限增长）。写路径 CAS（Compare ModRevision）
// 重试——集群形态 leader/follower 都挂 Web 都会写审计，读改写并发以 CAS 串行化；
// 连续冲突放弃本次记录（审计尽力而为：写失败仅日志告警，不阻塞操作）。
// 单 key 尚不存在时，先把前缀下已散落的单事件 key 合并进来再删除（首次写入
// 一次性收敛到环形形态）。

const (
	auditRingKey     = "/events/audit"
	auditRingMax     = 100
	auditCASMaxTries = 100 // 冲突=重读重追加（单次两个 etcd 操作、毫秒级）；上限兜底 ctx 超时
	auditCASWait     = 10 * time.Millisecond
)

// AuditStore 审计 etcd 存储。
type AuditStore struct {
	kv     *etcdx.KV
	prefix string
	node   string

	mu       sync.Mutex
	seq      int64
	migrated bool // 旧散 key 已合并（每进程一次）
}

// NewAuditStore 构造（prefix = {cluster 前缀}）。
func NewAuditStore(kv *etcdx.KV, prefix, nodeID string) *AuditStore {
	return &AuditStore{kv: kv, prefix: prefix, node: nodeID}
}

type auditEntry struct {
	Who    string    `json:"who"`
	What   string    `json:"what"`
	Result string    `json:"result"`
	Node   string    `json:"node"`
	Source string    `json:"source,omitempty"` // 源级操作填 sid；cluster 级留空（前端显示 —）
	At     time.Time `json:"at"`
}

// ringAppend 追加并截尾至 auditRingMax（纯函数，便于单测）。
func ringAppend(old []auditEntry, e auditEntry) []auditEntry {
	out := append(old, e)
	if len(out) > auditRingMax {
		out = out[len(out)-auditRingMax:]
	}
	return out
}

func (a *AuditStore) appendAudit(who, what, result string, at time.Time) error {
	return a.appendAuditSource(who, "", what, result, at)
}

// appendAuditSource 源级审计写入（sid 空 = cluster 级操作）。
func (a *AuditStore) appendAuditSource(who, sid, what, result string, at time.Time) error {
	a.mu.Lock()
	a.seq++
	e := auditEntry{Who: who, What: what, Result: result, Node: a.node, Source: sid, At: at}
	a.mu.Unlock()

	actx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := a.prefix + auditRingKey
	var lastErr error
	for attempt := 0; attempt < auditCASMaxTries; attempt++ {
		if attempt > 0 {
			select {
			case <-actx.Done():
				return lastErr
			case <-time.After(auditCASWait):
			}
		}
		resp, err := a.kv.Get(actx, key)
		if err != nil {
			lastErr = err
			continue
		}
		var entries []auditEntry
		rev := int64(0)
		if len(resp.Kvs) > 0 {
			rev = resp.Kvs[0].ModRevision
			if err := json.Unmarshal(resp.Kvs[0].Value, &entries); err != nil {
				return fmt.Errorf("audit: 环形记录解析失败: %w", err) // 环形记录损坏时拒绝覆盖（防误写扩散坏数据）
			}
		} else if !a.migrated {
			// 单 key 尚不存在：尝试把旧散 key 合并进来（尽力一次）
			legacy, lerr := a.loadLegacy(actx)
			if lerr == nil && len(legacy) > 0 {
				entries = legacy
			}
		}
		buf, err := json.Marshal(ringAppend(entries, e))
		if err != nil {
			return err
		}
		txnResp, err := a.kv.Txn(actx, func(t clientv3.Txn) clientv3.Txn {
			return t.If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).
				Then(clientv3.OpPut(key, string(buf)))
		})
		if err != nil {
			lastErr = err
			continue
		}
		if txnResp.Succeeded {
			a.setMigrated()
			a.cleanupLegacy(actx) // 已合并进单 key，删旧散 key（key 不存在时为 no-op）
			return nil
		}
		lastErr = fmt.Errorf("audit: CAS 冲突（并发写）") // 重试
	}
	return lastErr
}

// setMigrated 标记迁移完成（只置位，避免每次写都做前缀探测）。
func (a *AuditStore) setMigrated() {
	a.mu.Lock()
	a.migrated = true
	a.mu.Unlock()
}

// loadLegacy 读散落单事件 key（{prefix}/events/audit/*，按 key 时间序）。
func (a *AuditStore) loadLegacy(ctx context.Context) ([]auditEntry, error) {
	resp, err := a.kv.Get(ctx, a.prefix+auditRingKey+"/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make([]auditEntry, 0, len(resp.Kvs))
	for _, k := range resp.Kvs {
		var e auditEntry
		if err := json.Unmarshal(k.Value, &e); err == nil {
			out = append(out, e)
		}
	}
	if len(out) > auditRingMax {
		out = out[len(out)-auditRingMax:]
	}
	return out, nil
}

// cleanupLegacy 删除旧散 key（单 key 已就绪后的收尾；无旧数据为 no-op）。
func (a *AuditStore) cleanupLegacy(ctx context.Context) {
	_, _ = a.kv.Delete(ctx, a.prefix+auditRingKey+"/", clientv3.WithPrefix())
}

// List 最近 n 条审计（时间倒序）。
func (a *AuditStore) List(ctx context.Context, n int) ([]auditEntry, error) {
	resp, err := a.kv.Get(ctx, a.prefix+auditRingKey)
	if err != nil {
		return nil, err
	}
	var out []auditEntry
	if len(resp.Kvs) > 0 {
		if err := json.Unmarshal(resp.Kvs[0].Value, &out); err != nil {
			return nil, fmt.Errorf("audit: 环形记录解析失败: %w", err)
		}
	} else {
		out, _ = a.loadLegacy(ctx) // 未迁移形态的读取兼容
	}
	// 倒序取尾部（最新在前）
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	if len(out) > n {
		out = out[:n]
	}
	return out, nil
}

// AuditSink 审计存储接口（nil = 只写日志；注意 typed-nil 陷阱，构造侧保证
// 仅在真实存储存在时传入非 nil 接口值）。
type AuditSink interface {
	appendAudit(who, what, result string, at time.Time) error
	appendAuditSource(who, sid, what, result string, at time.Time) error
	List(ctx context.Context, n int) ([]auditEntry, error)
}

// Append 写一条审计（cmd 装配层用——键迁移等进程级操作的留痕；HTTP 侧
// 走包内闭包）。写失败仅返回错误，调用方按「审计尽力而为」告警不阻塞。
func (a *AuditStore) Append(who, what, result string) error {
	return a.appendAudit(who, what, result, time.Now())
}
