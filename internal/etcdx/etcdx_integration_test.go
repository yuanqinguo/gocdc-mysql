//go:build integration

// 集成测试：需要 Docker。运行：
//
//	go test ./internal/etcdx/ -tags=integration -count=1
//
// 镜像默认 docker.cnb.cool/qingflyfei/edu.image.base:etcd-3.5.16（客户端 v3.6
// 兼容 v3.5+ 服务端；ghcr.io 国内不可达故用国内镜像），可用环境变量
// GOCDC_TEST_ETCD_IMAGE 覆盖。
package etcdx

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/etcd"
)

const testEtcdImageEnv = "GOCDC_TEST_ETCD_IMAGE"

func testImage() string {
	if img := os.Getenv(testEtcdImageEnv); img != "" {
		return img
	}
	return "docker.cnb.cool/qingflyfei/edu.image.base:etcd-3.5.16"
}

// TestIntegrationEtcdX 全链路：建连 → KV 读写重试包装 → session/lease → 节点注册
// 与注销。对应 03 §5.3 条款在真实 etcd 上的验证。
func TestIntegrationEtcdX(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ctr, err := etcd.Run(ctx, testImage())
	if err != nil {
		t.Fatalf("启动 etcd 容器: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	endpoints, err := ctr.ClientEndpoints(ctx)
	if err != nil {
		t.Fatalf("获取 endpoints: %v", err)
	}

	cli, err := New(Options{Endpoints: endpoints})
	if err != nil {
		t.Fatalf("构造客户端: %v", err)
	}
	defer cli.Close()

	kv := NewKV(cli)

	// 1) KV 读写
	if _, err := kv.Put(ctx, "/gocdc/test/k", "v1"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	resp, err := kv.Get(ctx, "/gocdc/test/k")
	if err != nil || len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "v1" {
		t.Fatalf("Get 回读失败: resp=%v err=%v", resp, err)
	}

	// 2) session + 节点注册
	sess, err := NewSession(ctx, cli, 2)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	reg := NewRegistrar(kv, "/gocdc/test", NodeInfo{NodeID: "node-a", AdvertiseAddr: "ip:8060", Role: "follower"})
	if err := reg.Register(ctx, sess); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := reg.Update(ctx, sess, "leader"); err != nil {
		t.Fatalf("Update role: %v", err)
	}
	if resp, err := kv.Get(ctx, reg.Key()); err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("注册 key 应存在: err=%v", err)
	}

	// 3) 注销后 key 消失
	if err := reg.Deregister(ctx); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if resp, err := kv.Get(ctx, reg.Key()); err != nil || len(resp.Kvs) != 0 {
		t.Fatalf("注销后 key 应不存在: err=%v resp=%v", err, resp)
	}

	// 4) lease 过期删除：session 关闭（lease 撤销）后其上 key 被清除
	reg2 := NewRegistrar(kv, "/gocdc/test", NodeInfo{NodeID: "node-b"})
	if err := reg2.Register(ctx, sess); err != nil {
		t.Fatalf("Register node-b: %v", err)
	}
	if err := sess.Close(); err != nil {
		t.Fatalf("Close session: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := kv.Get(ctx, reg2.Key())
		if err != nil {
			t.Fatalf("Get node-b: %v", err)
		}
		if len(resp.Kvs) == 0 {
			break // lease 撤销后 key 已删
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease 过期后 key 仍存在: %s", reg2.Key())
		}
		time.Sleep(100 * time.Millisecond)
	}
}
