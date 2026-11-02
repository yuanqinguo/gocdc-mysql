package main

import (
	"hash/fnv"
	"testing"
)

// server_id 缺省派生：etcd 配置中心共享一份配置时，
// 各副本 node.id 不同 → 派生值天然不同；同节点稳定；0 结果防御。
func TestDeriveServerID(t *testing.T) {
	// 不同 node.id → 不同派生值（多副本共享配置不冲突的核心断言）
	seen := map[uint32]string{}
	for _, id := range []string{"node1", "node2", "node3", "gocdc-abc12345", "host-9f3a2b1c"} {
		sid := deriveServerID(id)
		if sid == 0 {
			t.Fatalf("node.id=%s 派生出非法 0", id)
		}
		if prev, dup := seen[sid]; dup {
			t.Fatalf("node.id=%s 与 %s 派生值碰撞：%d", id, prev, sid)
		}
		seen[sid] = id
	}
	// 同 node.id 稳定（重启不变）
	if deriveServerID("node1") != deriveServerID("node1") {
		t.Fatal("同 node.id 派生值应稳定")
	}
	// 派生算法锁定：fnv32a（防未来误改导致已部署集群派生值漂移互踢）
	h := fnv.New32a()
	_, _ = h.Write([]byte("node1"))
	if want := h.Sum32(); deriveServerID("node1") != want {
		t.Fatalf("派生算法应锁定 fnv32a(%q)=%d", "node1", want)
	}
	// 空 node.id：派生路径不 panic 且非 0（实际由 deriveNodeID 兜底随机）
	if sid := deriveServerID(""); sid == 0 {
		t.Fatal("空 node.id 派生值不应为 0")
	}
}

// 多源 server_id 派生（PRD 05 §6）：同进程各源按 node.id:sid 派生天然互异；
// 同源多副本（node.id 不同）互异——与现行派生的两条性质同构（MS-15）。
func TestDeriveServerID_MultiSource(t *testing.T) {
	const node = "node1"
	sids := []string{"default", "order-center", "user-center", "billing", "log"}
	seen := map[uint32]string{}
	for _, sid := range sids {
		id := deriveServerID(node + ":" + sid)
		if id == 0 {
			t.Fatalf("source=%s 派生出非法 0", sid)
		}
		if prev, dup := seen[id]; dup {
			t.Fatalf("同进程两源派生值碰撞：%s vs %s（%d）", prev, sid, id)
		}
		seen[id] = sid
	}
	// 同源多副本：node.id 不同 → 派生值不同（HA 共享配置场景）
	if deriveServerID("node1:default") == deriveServerID("node2:default") {
		t.Fatal("同源不同副本的派生值不应相同")
	}
	// 派生算法锁定：fnv32a(node_id:sid)
	h := fnv.New32a()
	_, _ = h.Write([]byte("node1:order-center"))
	if want := h.Sum32(); deriveServerID("node1:order-center") != want {
		t.Fatalf("派生算法应锁定 fnv32a(%q)=%d", "node1:order-center", want)
	}
}
