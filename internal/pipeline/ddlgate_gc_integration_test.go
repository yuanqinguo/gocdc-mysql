//go:build integration

// DDL 已决记录 GC 集成测试（P2）：过期已决删除，pending 与新决保留。
package pipeline

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/etcd"

	"gocdc/internal/etcdx"
)

func ddlGCImage() string {
	if img := os.Getenv("GOCDC_TEST_ETCD_IMAGE"); img != "" {
		return img
	}
	return "docker.cnb.cool/qingflyfei/edu.image.base:etcd-3.5.16"
}

func TestIntegrationDDLGateGC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctr, err := etcd.Run(ctx, ddlGCImage())
	if err != nil {
		t.Fatalf("启动 etcd 容器: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	endpoints, err := ctr.ClientEndpoints(ctx)
	if err != nil {
		t.Fatalf("获取 endpoints: %v", err)
	}
	cli, err := etcdx.New(etcdx.Options{Endpoints: endpoints})
	if err != nil {
		t.Fatalf("构造客户端: %v", err)
	}
	defer cli.Close()

	gate := NewDDLGate(etcdx.NewKV(cli), "/gocdc/ddlgc/events/ddl")
	old := time.Now().Add(-8 * 24 * time.Hour)
	fresh := time.Now()
	// pending（永不删）/ 8 天前已决（删）/ 刚决（保留）
	recs := []DDLRecord{
		{ID: "db1.t1.f.1", Status: DecisionPending, DecidedAt: old},
		{ID: "db1.t2.f.2", Status: DecisionApprove, DecidedAt: old},
		{ID: "db1.t3.f.3", Status: DecisionSkip, DecidedAt: fresh},
		{ID: "db1.t4.f.4", Status: DecisionSynced, DecidedAt: old},
	}
	for _, r := range recs {
		if err := gate.Record(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	removed, err := gate.GC(ctx, DDLGCRetention)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Fatalf("应删 2 条过期已决（t2/t4），实际 %d", removed)
	}
	if _, ok, _ := gate.Settled(ctx, "db1.t2.f.2"); ok {
		t.Fatal("过期 approve 应已删除")
	}
	if _, ok, _ := gate.Settled(ctx, "db1.t3.f.3"); !ok {
		t.Fatal("新决应保留")
	}
	// pending 即使 DecidedAt 很旧也不删（Record 不覆盖已存在记录，状态仍 pending）
	if _, ok, _ := gate.Settled(ctx, "db1.t1.f.1"); ok {
		t.Fatal("pending 不应被 GC")
	}
	list, err := gate.PendingByTable(ctx, "", "")
	if err != nil || len(list) != 1 || list[0].ID != "db1.t1.f.1" {
		t.Fatalf("pending 应保留: %v %d", err, len(list))
	}
	// 二次 GC 幂等
	if n, err := gate.GC(ctx, DDLGCRetention); err != nil || n != 0 {
		t.Fatalf("二次 GC 应幂等: n=%d err=%v", n, err)
	}
}
