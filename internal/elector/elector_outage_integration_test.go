//go:build integration

// 集成回归：失联停管线的时序不变式（03 §4.2-1）——
//
//	在位复核必须带超时：etcd 分区/宕机时裸 cli.Get 会阻塞在 balancer 重试上，
//	leader 迟迟不失位、管线持续投递；
//	OnLost 必须在 TTL + checkTimeout + 裕量内到达。
//
// 本测试：单节点当选 → 停 etcd 容器 → OnLost 必须在 8s 内触发（TTL=4s +
// checkTimeout=1s + 裕量）；恢复 etcd → 自动重新当选（elector 循环自愈）。
package elector

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/etcd"

	"gocdc/internal/etcdx"
)

func TestIntegrationElector_EtcdOutageOnLostTiming(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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

	cli, err := etcdx.New(etcdx.Options{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	var (
		mu      sync.Mutex
		lostAt  time.Time
		lostWhy string
	)
	// TTL=4s、checkInterval=500ms → checkTimeout=min(1s, 2s)=1s
	el := New(cli, Options{
		Prefix: "/gocdc/outage", NodeID: "node-a", AdvertiseAddr: "a:8060",
		LeaseTTL: 4, LeaderCheckInterval: 500 * time.Millisecond,
		OnLost: func(reason string) {
			mu.Lock()
			if lostAt.IsZero() {
				lostAt = time.Now()
				lostWhy = reason
			}
			mu.Unlock()
		},
	})

	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	errCh := make(chan error, 1)
	go func() { errCh <- el.Run(runCtx) }()

	// 等待首次当选
	deadline := time.Now().Add(15 * time.Second)
	for el.Role() != RoleLeader {
		if time.Now().After(deadline) {
			t.Fatal("15s 内未当选")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// ── 停 etcd（1s 强制超时）→ OnLost 必须在 8s 内触发 ──
	stopAt := time.Now()
	to := 1 * time.Second
	if err := ctr.Stop(context.Background(), &to); err != nil {
		t.Fatalf("停止 etcd: %v", err)
	}
	deadline = stopAt.Add(8 * time.Second) // TTL(4) + checkTimeout(1) + 裕量(3)
	for {
		mu.Lock()
		got := !lostAt.IsZero()
		mu.Unlock()
		if got {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("etcd 停止后 8s 内未失位（P0-3 回归失败：复核仍阻塞）")
		}
		time.Sleep(100 * time.Millisecond)
	}
	mu.Lock()
	delay := lostAt.Sub(stopAt)
	why := lostWhy
	mu.Unlock()
	t.Logf("OnLost 时延=%v reason=%q", delay, why)
	if delay > 8*time.Second {
		t.Fatalf("失位时延 %v 超上界 8s", delay)
	}

	// 注：etcd 恢复后自动重选（elector 循环自愈）由 L3 真实验收覆盖
	// （VERIFICATION D6：恢复后 15s 内重选成功）——testcontainers 容器
	// stop/start 复用在本镜像下不可靠（数据目录重启失败），不在 L2 断言。

	stop()
	select {
	case <-errCh:
	case <-time.After(10 * time.Second):
		t.Fatal("elector 未退出")
	}
}
