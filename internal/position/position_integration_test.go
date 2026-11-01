//go:build integration

// 集成测试：需要 Docker。运行：
//
//	go test ./internal/position/ -tags=integration -count=1
//
// 镜像默认 docker.cnb.cool/qingflyfei/edu.image.base:etcd-3.5.16，
// 可用 GOCDC_TEST_ETCD_IMAGE 覆盖。
package position

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"

	"gocdc/internal/etcdx"
	"gocdc/internal/model"
)

func TestIntegrationStoreGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	image := os.Getenv("GOCDC_TEST_ETCD_IMAGE")
	if image == "" {
		image = "docker.cnb.cool/qingflyfei/edu.image.base:etcd-3.5.16"
	}
	ctr, err := etcd.Run(ctx, image)
	if err != nil {
		t.Fatalf("启动 etcd 容器: %v", err)
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

	// 进程 lease（节点注册/选举同一 lease 家族；TTL 短便于过期验证）
	sess, err := etcdx.NewSession(ctx, cli, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	const (
		posKey  = "/gocdc/c1/position"
		gateKey = "/gocdc/c1/election"
	)
	// gateKey 挂到本 lease（模拟选主成功后 leader 的 election key）
	if _, err := cli.Put(ctx, gateKey, "leader=n1", clientv3.WithLease(sess.Lease())); err != nil {
		t.Fatal(err)
	}

	kv := etcdx.NewKV(cli)
	s := NewStore(kv, posKey, gateKey, sess.Lease())

	// 有 gate：写成功 → 读回一致
	p := model.Position{Name: "bin.000001", Pos: 100, Seq: 1, NodeID: "n1"}
	if err := s.Save(ctx, p); err != nil {
		t.Fatalf("gate 存活时应写入: %v", err)
	}
	got, ok, err := s.Load(ctx)
	if err != nil || !ok || got.Pos != 100 {
		t.Fatalf("load: %v %v %+v", ok, err, got)
	}

	// 撤销 lease（模拟失主/etcd 分区）→ gateKey 消失 → 写被拒
	if _, err := cli.Revoke(ctx, sess.Lease()); err != nil {
		t.Fatal(err)
	}
	err = s.Save(ctx, p)
	if err == nil {
		t.Fatal("lease 撤销后写入应被拒")
	}
	t.Logf("拒错形态: %v", err)
}
