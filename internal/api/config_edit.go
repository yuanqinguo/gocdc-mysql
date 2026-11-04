// 配置编辑与重载（13 §3）：Web 规则 CRUD 的服务端链路——
//
//	GET  /config/raw           配置中心原始 YAML + CAS base（revision）；
//	PUT  /sources/:sid/rules   整组提交：read → 节点级 splice → 全量加载链
//	                           校验 → 活体预检 → CAS 写 → 审计（check_only=true
//	                           同链路预演，不写不占 confirm）；
//	POST /config/apply-restart 优雅排空退出（SIGTERM 同链路），由编排拉起。
//
// 风险方向说明（保存语义）：加表=多同步（幂等方向）、减表=少同步（位点照走，
// 目标数据不动）——写错 rules 的爆炸半径是「少同步/晚同步」（可补救），与
// target 连接改错（写错库，不可逆）不同级，故 source/target 不进本链路、
// 保持锁定（只读展示 + 连接测试例外）。
package api

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"gocdc/internal/config"
	"gocdc/internal/etcdx"
	"gocdc/internal/logging"
)

// ErrConfigCASConflict PUT base 与配置中心现值不一致（并发修改，读方刷新重试）。
var ErrConfigCASConflict = errors.New("配置已被他人修改（CAS 冲突），请刷新后重试")

// ConfigStore 配置中心原始正文读写。etcd 形态注入实现；file 形态必须保持
// nil 接口值（raw/PUT 显式 409 指引直接编辑文件）——实现侧警惕 typed-nil：
// 不要把非 nil 指针塞进 nil 接口。
type ConfigStore interface {
	// ReadRaw 原始正文 + 该 key 的 ModRevision（作 PUT base）。
	ReadRaw(ctx context.Context) (revision int64, raw []byte, err error)
	// CASWrite base 不一致返回 ErrConfigCASConflict；成功返回新 revision。
	CASWrite(ctx context.Context, base int64, raw []byte) (newRevision int64, err error)
}

// PreflightFunc 活体预检闭包（cmd 装配：源/目标连接核对 diff 出的变更表）。
// nil = 未装配（报告注明跳过，不阻断保存——预检是加一道闸不是唯一防线，
// 重启时的启动核对仍会全表 fail-fast）。
type PreflightFunc func(ctx context.Context, sid string, oldRules, newRules []config.Rule) PreflightReport

// EtcdConfigStore ConfigStore 的 etcd 实现（配置中心 key；KV 重试包装吸收
// 瞬时故障——重试对 CAS 安全，冲突非错误不触发重试路径）。
type EtcdConfigStore struct {
	kv  *etcdx.KV
	key string
}

// NewEtcdConfigStore 构造。
func NewEtcdConfigStore(kv *etcdx.KV, key string) *EtcdConfigStore {
	return &EtcdConfigStore{kv: kv, key: key}
}

// ReadRaw 原始正文 + ModRevision（作 PUT base）。
func (s *EtcdConfigStore) ReadRaw(ctx context.Context) (int64, []byte, error) {
	resp, err := s.kv.Get(ctx, s.key)
	if err != nil {
		return 0, nil, err
	}
	if len(resp.Kvs) == 0 {
		return 0, nil, fmt.Errorf("配置 %s 不存在（配置中心正文被删？）", s.key)
	}
	return resp.Kvs[0].ModRevision, resp.Kvs[0].Value, nil
}

// CASWrite base 一致才写（Compare ModRevision，与审计环形同款）；不一致返回
// ErrConfigCASConflict，成功返回写后 revision。
func (s *EtcdConfigStore) CASWrite(ctx context.Context, base int64, raw []byte) (int64, error) {
	resp, err := s.kv.Txn(ctx, func(t clientv3.Txn) clientv3.Txn {
		return t.If(clientv3.Compare(clientv3.ModRevision(s.key), "=", base)).
			Then(clientv3.OpPut(s.key, string(raw)))
	})
	if err != nil {
		return 0, err
	}
	if !resp.Succeeded {
		return 0, ErrConfigCASConflict
	}
	return resp.Header.Revision, nil
}

// applyRestartDelay 停机触发前的延迟（让 HTTP 响应完全写出；测试调短）。
var applyRestartDelay = 500 * time.Millisecond

// configRaw GET /api/v1/config/raw。
func (h apiHandlers) configRaw(c *gin.Context) {
	if h.ConfigStore == nil {
		abort(c, 409, "当前配置来源为本地文件（"+h.ConfigSrc+"），无配置中心正文可读；请直接编辑文件后重启进程")
		return
	}
	rev, raw, err := h.ConfigStore.ReadRaw(c.Request.Context())
	if err != nil {
		respondErr(c, 500, err.Error())
		return
	}
	respondOK(c, ConfigRawData{Revision: rev, YAML: string(raw), ConfigKey: h.ConfigKey})
}

// sourceGetRules GET /api/v1/sources/:sid/rules：配置中心中该源规则组的
// 原样 JSON（组形态，编辑器种子数据；与 PUT 提交形态对称）。file 形态 409。
func (h apiHandlers) sourceGetRules(c *gin.Context) {
	if sd := h.resolveSource(c); sd == nil {
		return
	} else if h.ConfigStore == nil {
		abort(c, 409, "当前配置来源为本地文件（"+h.ConfigSrc+"），无配置中心规则可读")
		return
	} else {
		rev, raw, err := h.ConfigStore.ReadRaw(c.Request.Context())
		if err != nil {
			respondErr(c, 500, err.Error())
			return
		}
		res := config.LoadFromRaw(raw, h.Cluster)
		if len(res.Errors) > 0 || res.Config == nil {
			respondErr(c, 500, "配置中心正文无法解析（先修复正文）: "+errsText(res.Errors))
			return
		}
		rules, ok := rulesOfSource(res.Config, sd.ID)
		if !ok {
			abort(c, 404, "配置正文中不存在 sources/"+sd.ID)
			return
		}
		respondOK(c, GetRulesResult{Revision: rev, Rules: rules})
	}
}

// sourcePutRules PUT /api/v1/sources/:sid/rules。
func (h apiHandlers) sourcePutRules(c *gin.Context) {
	if sd := h.resolveSource(c); sd != nil {
		h.putRulesOf(sd, c)
	}
}

// putRulesOf 规则整组保存主链路。
func (h apiHandlers) putRulesOf(sd *SourceDeps, c *gin.Context) {
	if h.ConfigStore == nil {
		abort(c, 409, "当前配置来源为本地文件（"+h.ConfigSrc+"），不可经 API 保存；请直接编辑文件后重启进程")
		return
	}
	checkOnly := c.Query("check_only") == "true"
	if !checkOnly && !requireConfirm(c) {
		return
	}
	base := int64(0)
	if !checkOnly {
		v, err := strconv.ParseInt(c.Query("base"), 10, 64)
		if err != nil || v <= 0 {
			abort(c, 400, "缺少合法 base 参数（先 GET /config/raw 取 revision，CAS 防并发覆盖）")
			return
		}
		base = v
	}
	var body PutRulesRequest
	if err := c.ShouldBindJSON(&body); err != nil {
		// Rule/TableRule 严格解码（未知字段/平铺写法）的指引文案在此透出
		abort(c, 400, err.Error())
		return
	}

	_, raw, err := h.ConfigStore.ReadRaw(c.Request.Context())
	if err != nil {
		respondErr(c, 500, "读取配置中心正文失败: "+err.Error())
		return
	}
	oldRes := config.LoadFromRaw(raw, h.Cluster)
	if len(oldRes.Errors) > 0 || oldRes.Config == nil {
		respondErr(c, 500, "现有配置正文无法通过加载链（先修复配置中心正文再编辑）: "+errsText(oldRes.Errors))
		return
	}
	oldRules, ok := rulesOfSource(oldRes.Config, sd.ID)
	if !ok {
		abort(c, 409, "配置正文中不存在 sources/"+sd.ID+"（正文被外部改动？刷新后重试）")
		return
	}

	newRaw, err := config.SpliceSourceRules(raw, sd.ID, body.Rules)
	if err != nil {
		abort(c, 400, err.Error())
		return
	}
	newRes := config.LoadFromRaw(newRaw, h.Cluster)
	if len(newRes.Errors) > 0 {
		respondErrData(c, 400, "配置校验未通过（未保存）: "+errsText(newRes.Errors), nil)
		return
	}

	added, removed, modified := config.DiffRules(oldRules, body.Rules)
	var report PreflightReport
	if h.Preflight != nil {
		report = h.Preflight(c.Request.Context(), sd.ID, oldRules, body.Rules)
	} else {
		report.Warnings = []string{"预检未装配，跳过活体核对（重启时启动核对仍会全表把关）"}
	}
	result := PutRulesResult{Added: added, Removed: removed, Modified: modified, Report: &report}

	if checkOnly { // 预演：同一校验链，不写不占 confirm
		respondOK(c, result)
		return
	}
	if len(report.Errors) > 0 {
		respondErrData(c, 422, "活体预检未通过（未保存；修复问题或调整规则后重试）", result)
		return
	}

	newRev, werr := h.ConfigStore.CASWrite(c.Request.Context(), base, newRaw)
	detail := diffSummary(sd.ID, added, removed, modified)
	if werr != nil {
		auditSource(auditOrNil(h.Audit), c, sd.ID, auditConfigRulesUpdate, detail, werr)
		if errors.Is(werr, ErrConfigCASConflict) {
			respondErr(c, 409, werr.Error())
		} else {
			respondErr(c, 500, werr.Error())
		}
		return
	}
	auditSource(auditOrNil(h.Audit), c, sd.ID, auditConfigRulesUpdate, detail, nil)
	result.Revision = newRev
	respondOK(c, result)
}

// applyRestart POST /api/v1/config/apply-restart：优雅排空退出（与 SIGTERM
// 同一条 drain 链路：停拉取→排空→位点终值→resign），由编排层拉起以应用
// 新配置。集群滚动序：先逐个重启 follower（带新配置回来当热备），最后重启
// leader——顺序颠倒不产生数据风险（旧配置=少同步，安全方向），只浪费一轮。
func (h apiHandlers) applyRestart(c *gin.Context) {
	if h.Shutdown == nil {
		abort(c, 409, "本节点未接入停机触发，请用编排层命令重启进程（docker compose restart / k8s rollout restart）")
		return
	}
	if !requireConfirm(c) {
		return
	}
	detail := "node=" + h.NodeID
	audit(auditOrNil(h.Audit), c, auditConfigApplyRestart, detail, nil)
	respondDone(c)
	go func() { // 响应先写出，再触发停机
		time.Sleep(applyRestartDelay)
		logging.L().Warn("apply-restart：优雅停机开始（Web 触发）——排空+位点终值+resign 后退出，等编排拉起",
			zap.String("node", h.NodeID))
		h.Shutdown()
	}()
}

// errsText 错误清单拼接（Web 响应文案用；空清单兜底通用文案）。
func errsText(errs []error) string {
	if len(errs) == 0 {
		return "原因未知（见服务端日志）"
	}
	msgs := make([]string, 0, len(errs))
	for _, e := range errs {
		msgs = append(msgs, e.Error())
	}
	return strings.Join(msgs, "；")
}

// rulesOfSource 配置中指定源的规则组（无该源 ok=false）。
func rulesOfSource(cfg *config.Config, sid string) ([]config.Rule, bool) {
	for i := range cfg.Sources {
		if cfg.Sources[i].ID == sid {
			return cfg.Sources[i].Rules, true
		}
	}
	return nil, false
}

// diffSummary 审计 detail：sources/{sid} +a/-r/~m 表清单（每类最多列 5 个防刷屏）。
func diffSummary(sid string, added, removed, modified []config.TableRef) string {
	var b strings.Builder
	fmt.Fprintf(&b, "sources/%s +%d/-%d/~%d", sid, len(added), len(removed), len(modified))
	appendRefs := func(label string, refs []config.TableRef) {
		if len(refs) == 0 {
			return
		}
		b.WriteString(" " + label)
		for i, r := range refs {
			if i == 5 {
				fmt.Fprintf(&b, " …(+%d more)", len(refs)-5)
				break
			}
			b.WriteString(" " + r.Schema + "." + r.Table)
		}
	}
	appendRefs("+", added)
	appendRefs("-", removed)
	appendRefs("~", modified)
	return b.String()
}
