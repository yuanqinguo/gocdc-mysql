//go:build integration

// stock 死标记 GC 集成测试（P2）：白名单外表的标记删除；白名单内与全局位点保留。
package stock

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"

	"gocdc/internal/config"
	"gocdc/internal/etcdx"
	"gocdc/internal/model"
	"gocdc/internal/pipeline"
)

func gcTestImage() string {
	if img := os.Getenv("GOCDC_TEST_ETCD_IMAGE"); img != "" {
		return img
	}
	return "docker.cnb.cool/qingflyfei/edu.image.base:etcd-3.5.16"
}

func TestIntegrationMarkerGC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctr, err := etcd.Run(ctx, gcTestImage())
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
	kv := etcdx.NewKV(cli)

	markers := NewMarkerStore(kv, "/gocdc/markerGC/stock")
	// 写门槛：与当选同构（session lease + leader 指针 key）
	gateSess, err := etcdx.NewSession(ctx, cli, 60)
	if err != nil {
		t.Fatal(err)
	}
	defer gateSess.Close()
	if _, err := cli.Put(ctx, "/gocdc/markerGC/leader", "node-a", clientv3.WithLease(gateSess.Lease())); err != nil {
		t.Fatal(err)
	}
	markers.SetGate("/gocdc/markerGC/leader", gateSess.Lease())

	doTrue := true
	rules, err := pipeline.CompileRules([]config.Rule{{
		Schema: "srcdb", Sinks: &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
		Tables: []config.TableRule{{Table: "t_keep", OrderByColumn: "id", DoStock: &doTrue}},
	}})
	if err != nil {
		t.Fatal(err)
	}

	// 播种：白名单内 done / 白名单外 done 两枚
	for _, tbl := range []string{"t_keep", "t_dead", "t_gone"} {
		if err := markers.Put(ctx, "srcdb", tbl, &Marker{Status: StatusDone, RowsDone: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := markers.PutGlobalPosition(ctx, model.Position{Name: "binlog.000001", Pos: 4}); err != nil {
		t.Fatal(err)
	}

	// 失败诊断：GC 前倾倒 prefix 下全部 key 与 keep 判定
	if dbg, derr := kv.Get(ctx, "/gocdc/markerGC/stock/", clientv3.WithPrefix()); derr == nil {
		for _, k := range dbg.Kvs {
			suffix := strings.ToLower(strings.TrimPrefix(string(k.Key), "/gocdc/markerGC/stock/"))
			t.Logf("GC 前 key=%s keep=%v", string(k.Key), rules.MatchByKey(suffix) != nil)
		}
	} else {
		t.Fatalf("诊断列举失败: %v", derr)
	}
	removed, err := markers.GC(ctx, func(dbTable string) bool {
		return rules.MatchByKey(dbTable) != nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Fatalf("应删 2 个死标记（t_dead/t_gone），实际 %d", removed)
	}
	if mk, _ := markers.Get(ctx, "srcdb", "t_keep"); mk == nil || mk.Status != StatusDone {
		t.Fatal("白名单内标记应保留")
	}
	if mk, _ := markers.Get(ctx, "srcdb", "t_dead"); mk != nil {
		t.Fatal("白名单外标记应删除")
	}
	if p, ok, _ := markers.GlobalPosition(ctx); !ok || p.Name != "binlog.000001" {
		t.Fatal("全局位点应保留")
	}
	// 二次 GC：幂等零删除
	if n, err := markers.GC(ctx, func(dbTable string) bool { return rules.MatchByKey(dbTable) != nil }); err != nil || n != 0 {
		t.Fatalf("二次 GC 应幂等: n=%d err=%v", n, err)
	}
}
