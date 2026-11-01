//go:build integration

// 审计环形存储集成测试：单 key 环形 100 条 + CAS 并发 + 散 key 一次性迁移。
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"

	"gocdc/internal/etcdx"
)

func auditTestImage() string {
	if img := os.Getenv("GOCDC_TEST_ETCD_IMAGE"); img != "" {
		return img
	}
	return "docker.cnb.cool/qingflyfei/edu.image.base:etcd-3.5.16"
}

func auditTestEtcd(t *testing.T) *etcdx.KV {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctr, err := etcd.Run(ctx, auditTestImage())
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
	t.Cleanup(func() { cli.Close() })
	return etcdx.NewKV(cli)
}

// TestIntegrationAuditRing 单 key 环形：超限截尾、CAS 并发不丢条目、List 倒序。
func TestIntegrationAuditRing(t *testing.T) {
	kv := auditTestEtcd(t)
	a1 := NewAuditStore(kv, "/gocdc/auditring", "node-a")
	a2 := NewAuditStore(kv, "/gocdc/auditring", "node-b")

	// 并发写：两个节点各 30 条（模拟集群双 Web 写）
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(2)
		go func(i int) { defer wg.Done(); _ = a1.appendAudit("u", fmt.Sprintf("op-a-%d", i), "ok", time.Now()) }(i)
		go func(i int) { defer wg.Done(); _ = a2.appendAudit("u", fmt.Sprintf("op-b-%d", i), "ok", time.Now()) }(i)
	}
	wg.Wait()

	resp, err := kv.Get(context.Background(), "/gocdc/auditring/events/audit")
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("单 key 形态: err=%v kvs=%d", err, len(resp.Kvs))
	}
	var entries []auditEntry
	if err := json.Unmarshal(resp.Kvs[0].Value, &entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 60 {
		t.Fatalf("并发写应无丢失: %d", len(entries))
	}
	// 散 key 应已清理
	legacy, err := kv.Get(context.Background(), "/gocdc/auditring/events/audit/", clientv3.WithPrefix())
	if err != nil || len(legacy.Kvs) != 0 {
		t.Fatalf("散 key 未清理: %v %d", err, len(legacy.Kvs))
	}
	// List：条数上限 + 无重复（并发下条目时间戳在入队时取、数组序由 CAS 提交序
	// 决定——时间戳单调性只在顺序写下成立，单独见下方 seq 段）
	list, err := a1.List(context.Background(), 10)
	if err != nil || len(list) != 10 {
		t.Fatalf("List: %v %d", err, len(list))
	}
	seenOps := map[string]bool{}
	for _, e := range list {
		if seenOps[e.What] {
			t.Fatalf("重复条目: %s", e.What)
		}
		seenOps[e.What] = true
	}

	// 顺序写：List 应严格最新在前
	a3 := NewAuditStore(kv, "/gocdc/auditring-seq", "node-seq")
	for i := 0; i < 5; i++ {
		if err := a3.appendAudit("u", fmt.Sprintf("seq-%d", i), "ok", time.Now()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // 保证 At 纳秒可分辨
	}
	seqList, err := a3.List(context.Background(), 3)
	if err != nil || len(seqList) != 3 {
		t.Fatalf("seq List: %v %d", err, len(seqList))
	}
	for i, want := range []string{"seq-4", "seq-3", "seq-2"} {
		if seqList[i].What != want {
			t.Fatalf("顺序写应最新在前: [%d]=%s 期望 %s", i, seqList[i].What, want)
		}
	}
}

// TestIntegrationAuditLegacyMigration 旧散 key 首次写入时合并进单 key 并删除。
func TestIntegrationAuditLegacyMigration(t *testing.T) {
	kv := auditTestEtcd(t)
	prefix := "/gocdc/auditlegacy"
	// 播种旧形态散 key（appendAudit 只 Put 从不删的历史残留）
	for i := 0; i < 3; i++ {
		e := auditEntry{Who: "u", What: fmt.Sprintf("legacy-%d", i), Node: "old", At: time.Now().Add(time.Duration(i) * time.Second)}
		buf, _ := json.Marshal(e)
		if _, err := kv.Put(context.Background(), fmt.Sprintf("%s/events/audit/2026010100000%d-old-%d", prefix, i, i), string(buf)); err != nil {
			t.Fatal(err)
		}
	}
	a := NewAuditStore(kv, prefix, "node-new")
	if err := a.appendAudit("u", "new-entry", "ok", time.Now()); err != nil {
		t.Fatalf("首次写入: %v", err)
	}
	// 旧散 key 已删、单 key 含 4 条（3 旧 + 1 新）
	legacy, _ := kv.Get(context.Background(), prefix+"/events/audit/", clientv3.WithPrefix())
	if len(legacy.Kvs) != 0 {
		t.Fatalf("旧散 key 应已删除: %d", len(legacy.Kvs))
	}
	list, err := a.List(context.Background(), 100)
	if err != nil || len(list) != 4 {
		t.Fatalf("合并后应 4 条: %v %d", err, len(list))
	}
}
