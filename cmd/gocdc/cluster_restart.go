// 服务级重启编排（2026-11-12 用户决策「应用并重启=服务级，集群全部节点」）：
// 接住请求的节点作为编排者——列举节点注册表 peers（排除自己）→ follower 先
// leader 后逐个向 peer Web API 发标准 apply-restart（peer 各自优雅排空退出、
// 由编排拉起、带新配置回来）→ 轮询 peer /healthz 恢复 → 自己最后重启。
//
// 数据安全定论（RUNBOOK §9.1-2）：重启顺序颠倒不产生数据风险（旧配置=少同步，
// 安全方向），只浪费一轮——故 self-last 仅保证编排者活到收尾，peer 内部排序
// 尽量贴滚动序但非安全前提。单节点部署：注册表只有自己 → 无 peers，行为与
// 旧「重启本节点」完全一致。
//
// 失败语义：任一 peer 失败/超时**不中止**编排（用户显式操作；成功的 peer 已
// 带新配置运行，混跑状态只能靠重启其余节点收敛）——失败节点逐个记入结果，
// 由前端醒目展示、人工处置（该节点无编排拉起/凭据不同步等）。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"gocdc/internal/api"
	"gocdc/internal/config"
	"gocdc/internal/etcdx"
	"gocdc/internal/logging"
)

// 结果类型见 internal/api/dto.go（PeerRestartResult/ClusterRestartResult，
// api 响应与 cmd 编排共用单一事实源）。

// clusterRestarter 服务级重启编排器（cmd 层闭包注入 api.Deps.ClusterRestart）。
type clusterRestarter struct {
	cfg         *config.Config
	kv          *etcdx.KV
	prefix      string // 集群前缀 /gocdc/{cluster}
	nodeID      string // 自己（排除 + self-last）
	stop        func() // 自身优雅停机（与 SIGTERM 同链路）
	client      *http.Client
	healthLimit time.Duration // 单 peer 健康等待上限（测试调短）
}

func newClusterRestarter(cfg *config.Config, kv *etcdx.KV, prefix, nodeID string, stop func()) *clusterRestarter {
	return &clusterRestarter{
		cfg: cfg, kv: kv, prefix: prefix, nodeID: nodeID, stop: stop,
		client:      &http.Client{Timeout: 15 * time.Second},
		healthLimit: 90 * time.Second,
	}
}

// restartService 服务级重启入口（api.Deps.ClusterRestart 闭包目标）：触发每个
// peer 重启并等其 healthz 恢复。返回逐 peer 结果；不触发自身停机（调用方在
// HTTP 响应写出后调 stop）。列举失败返回 error——调用方不得把「列举失败」
// 当作「单节点」静默降级（那会跳过全部 peer，把服务级重启悄悄变成单节点重启，
// 其余节点留在旧配置——2026-11-12 集群端到端实测抓到的缺陷）。
func (cr *clusterRestarter) restartService(ctx context.Context) (api.ClusterRestartResult, error) {
	peers, err := cr.listPeers(ctx)
	if err != nil {
		return api.ClusterRestartResult{}, fmt.Errorf("列举节点注册表失败（未触发任何重启，可重试）: %w", err)
	}
	return cr.fanoutPeers(ctx, peers), nil
}

// fanoutPeers 逐 peer 编排（无 peer = 单节点语义）。
func (cr *clusterRestarter) fanoutPeers(ctx context.Context, peers []registryPeer) api.ClusterRestartResult {
	res := api.ClusterRestartResult{Scope: "cluster", Self: "本节点最后重启（响应返回后自动执行）"}
	if len(peers) == 0 {
		res.Peers = []api.PeerRestartResult{}
		res.Self = "单节点部署：仅重启本节点"
		return res
	}
	for _, p := range peers {
		res.Peers = append(res.Peers, cr.restartPeer(ctx, p))
	}
	return res
}

// registryPeer 注册表条目（NodeInfo + 解析后的可达地址）。
type registryPeer struct {
	NodeInfo etcdx.NodeInfo
	Addr     string // http://{advertise_addr}
}

// listPeers 列举注册表 peers（排除自己），follower 先 leader 后
// （贴滚动序：带新配置的 follower 先回来当热备，leader 最后切换）。
// etcd 读取重试 3 次——瞬时失败必须显式上抛，绝不静默降级成「无 peers」
// （那会把服务级重启悄悄变成单节点重启，其余节点留在旧配置）。
func (cr *clusterRestarter) listPeers(ctx context.Context) ([]registryPeer, error) {
	var peers []registryPeer
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
		resp, err := cr.kv.Get(ctx, cr.prefix+"/nodes/", clientv3.WithPrefix())
		if err != nil {
			lastErr = err
			logging.L().Warn("服务级重启：列举节点注册表失败（重试）",
				zap.Int("attempt", attempt), zap.Error(err))
			continue
		}
		peers = nil
		for _, kvs := range resp.Kvs {
			var info etcdx.NodeInfo
			if json.Unmarshal(kvs.Value, &info) != nil || info.NodeID == cr.nodeID {
				continue
			}
			addr := normalizePeerAddr(info.AdvertiseAddr)
			if addr == "" {
				continue // 无可达地址（advertise 未配置且解析失败）——人工处置
			}
			peers = append(peers, registryPeer{NodeInfo: info, Addr: addr})
		}
		return sortPeers(peers), nil
	}
	return nil, lastErr
}

// sortPeers follower 先 leader 后（贴滚动序：带新配置的 follower 先回来当
// 热备，leader 最后切换）；同角色按 NodeID 稳定序。
func sortPeers(peers []registryPeer) []registryPeer {
	sort.SliceStable(peers, func(i, j int) bool {
		li, lj := peers[i].NodeInfo.Role == "leader", peers[j].NodeInfo.Role == "leader"
		if li != lj {
			return !li // leader 排后
		}
		return peers[i].NodeInfo.NodeID < peers[j].NodeInfo.NodeID
	})
	return peers
}

// normalizePeerAddr advertise_addr → 可达 http 基址。空值补「本机IP:web端口」
// 的解析在注册侧（bootstrap），此处只做形态归一（补 http:// 前缀、去尾斜杠）。
func normalizePeerAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	return strings.TrimRight(addr, "/")
}

// restartPeer 触发单 peer 优雅重启并等其 healthz 恢复（编排器拉起后新配置生效）。
func (cr *clusterRestarter) restartPeer(ctx context.Context, p registryPeer) api.PeerRestartResult {
	res := api.PeerRestartResult{NodeID: p.NodeInfo.NodeID, Addr: p.Addr, Role: p.NodeInfo.Role}
	if err := cr.triggerPeer(ctx, p.Addr); err != nil {
		res.Detail = "触发失败: " + err.Error()
		return res
	}
	if err := cr.waitPeerHealthy(ctx, p.Addr, cr.healthLimit); err != nil {
		res.Detail = "已触发退出，但 " + err.Error() + "（该节点可能无编排拉起，需人工启动）"
		return res
	}
	res.Ok = true
	return res
}

// triggerPeer 向 peer 发标准 apply-restart（共享配置 → 同一组 Web 凭据；
// peer 侧走同一条优雅排空链路并自行审计）。
func (cr *clusterRestarter) triggerPeer(ctx context.Context, addr string) error {
	url := addr + "/api/v1/config/apply-restart?confirm=true"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(nil))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if cr.cfg.Observability.Web.Username != "" {
		req.SetBasicAuth(cr.cfg.Observability.Web.Username, cr.cfg.Observability.Web.Password)
	}
	resp, err := cr.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

// waitPeerHealthy 轮询 peer /healthz 直到恢复 ok（进程退出→编排拉起→就绪）。
func (cr *clusterRestarter) waitPeerHealthy(ctx context.Context, addr string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	// 先等它真的退出（healthz 失败），再等回来——避免旧进程尚未退出就被判「已恢复」
	time.Sleep(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, addr+"/healthz", nil)
		if resp, err := cr.client.Do(req); err == nil {
			buf := new(bytes.Buffer)
			_, _ = buf.ReadFrom(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK && strings.TrimSpace(buf.String()) == "ok" {
				return nil
			}
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("健康等待超时（%v）", limit)
}
