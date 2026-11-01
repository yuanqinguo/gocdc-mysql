//go:build integration

// 集成测试：需要 Docker。运行：
//
//	go test ./internal/elector/ -tags=integration -count=1
//
// 镜像默认 docker.cnb.cool/qingflyfei/edu.image.base:etcd-3.5.16。
// 验收：kill leader 自动接管（< TTL+10s）；单 leader 不变量；
// 旧 leader 让位后位点写门槛失效（防双写）。
package elector

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/etcd"

	"gocdc/internal/etcdx"
	"gocdc/internal/model"
	"gocdc/internal/position"
)

func etcdImage() string {
	if v := os.Getenv("GOCDC_TEST_ETCD_IMAGE"); v != "" {
		return v
	}
	return "docker.cnb.cool/qingflyfei/edu.image.base:etcd-3.5.16"
}

type recorder struct {
	mu       sync.Mutex
	elected  []int64
	lost     []string
	isLeader bool
}

func (r *recorder) onElected(info Elected) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.elected = append(r.elected, info.Term)
	r.isLeader = true
}

func (r *recorder) onLost(reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lost = append(r.lost, reason)
	r.isLeader = false
}

func (r *recorder) leaderNow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.isLeader
}

func TestIntegrationElector_Takeover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ctr, err := etcd.Run(ctx, etcdImage())
	if err != nil {
		t.Fatalf("启动 etcd: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	endpoints, err := ctr.ClientEndpoints(ctx)
	if err != nil {
		t.Fatal(err)
	}

	mkNode := func(id string) (*Elector, *position.Store, context.CancelFunc, *recorder) {
		cli, err := etcdx.New(etcdx.Options{Endpoints: endpoints})
		if err != nil {
			t.Fatal(err)
		}
		rec := &recorder{}
		store := position.NewStore(etcdx.NewKV(cli), "/gocdc/ha/position", "", 0)
		el := New(cli, Options{
			Prefix: "/gocdc/ha", NodeID: id, AdvertiseAddr: id + ":8060",
			LeaseTTL: 3, LeaderCheckInterval: 300 * time.Millisecond,
			OnElected: func(info Elected) {
				rec.onElected(info)
				store.SetGate(info.LeaderKey, info.LeaseID) // 当选即切位点写门槛
			},
			OnLost: rec.onLost,
		})
		return el, store, func() { _ = cli.Close() }, rec
	}

	// ── 双节点启动 ──
	elA, storeA, closeA, recA := mkNode("node-a")
	elB, storeB, closeB, recB := mkNode("node-b")
	defer closeA()
	defer closeB()

	ctxA, stopA := context.WithCancel(context.Background())
	defer stopA() // 兜底释放（正常路径由 stopL 走 leader 停机流程）
	ctxB, stopB := context.WithCancel(context.Background())
	defer stopB()
	errA := make(chan error, 1)
	errB := make(chan error, 1)
	go func() { errA <- elA.Run(ctxA) }()
	go func() { errB <- elB.Run(ctxB) }()

	// 恰好一个 leader（10s 内）
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if elA.Role() == RoleLeader || elB.Role() == RoleLeader {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	aLeader, bLeader := elA.Role() == RoleLeader, elB.Role() == RoleLeader
	if aLeader == bLeader { // 同为 leader 或同为 follower 都错
		t.Fatalf("单 leader 不变量破坏: A=%v B=%v", aLeader, bLeader)
	}
	var (
		elL, elF       *Elector
		storeL, storeF *position.Store
		stopL          context.CancelFunc
		errL           chan error
		nameL          string
	)
	if aLeader {
		elL, storeL, stopL, errL, nameL = elA, storeA, stopA, errA, "node-a"
		elF, storeF = elB, storeB
	} else {
		elL, storeL, stopL, errL, nameL = elB, storeB, stopB, errB, "node-b"
		elF, storeF = elA, storeA
	}
	term1 := elL.Term()
	if term1 == 0 || elF.Term() != 0 {
		t.Fatalf("term 语义: leader=%d follower=%d", elL.Term(), elF.Term())
	}

	// follower 的位点写被拒（写门槛全覆盖，03 §4.2-2）
	if err := storeF.Save(ctx, modelPos("f", 1)); err == nil {
		t.Fatal("follower 位点写应被拒（无门槛）")
	}
	// leader 的位点写通过
	if err := storeL.Save(ctx, modelPos(nameL, 100)); err != nil {
		t.Fatalf("leader 位点写: %v", err)
	}

	// ── kill leader → 接管 < TTL+10s ──
	stopL() // ctx 取消 → Resign 让位（03 §4.2-4）
	select {
	case <-errL:
	case <-time.After(10 * time.Second):
		t.Fatal("旧 leader 未退出")
	}
	takeoverDeadline := time.Now().Add(3*time.Second + 10*time.Second)
	for time.Now().Before(takeoverDeadline) {
		if elF.Role() == RoleLeader {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if elF.Role() != RoleLeader {
		t.Fatal("新 leader 未在 TTL+10s 内接管")
	}
	if elF.Term() <= term1 {
		t.Fatalf("term 应单调递增: %d → %d", term1, elF.Term())
	}
	// leader 指针更新 + follower（旧 leader 已停）不复活管线——单 leader
	// 时序不变式：失位节点必须先收到 OnLost 停管线（recA/recB 的 isLeader
	// 互斥由单 leader 断言保证）；此处复核旧 leader 的 onLost 已发生
	recL := recA
	if nameL == "node-b" {
		recL = recB
	}
	recL.mu.Lock()
	lostSeen := len(recL.lost) > 0
	recL.mu.Unlock()
	if !lostSeen {
		t.Fatal("旧 leader 应收到 OnLost（停管线）")
	}

	// ── 旧 leader 的位点写门槛失效（lease 已易主）──
	if err := storeL.Save(ctx, modelPos(nameL, 200)); err == nil {
		t.Fatal("旧 leader 位点写应被拒（gate 已易主/失效）")
	}
	// 新 leader 可写
	if err := storeF.Save(ctx, modelPos("node-b", 300)); err != nil {
		t.Fatalf("新 leader 位点写: %v", err)
	}
}

// modelPos 测试位点。
func modelPos(name string, pos uint32) model.Position {
	return model.Position{Name: name, Pos: pos}
}
