// 节点身份派生：node.id 缺省生成与伪从库 server_id 派生（fnv32a，同节点稳定）。
package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"os"
)

// deriveNodeID 节点 ID（配置未填时派生 hostname-8位随机，k8s 多副本可用
// POD_NAME 注入保证唯一）。
func deriveNodeID() string {
	host, _ := os.Hostname()
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%s", host, hex.EncodeToString(b[:]))
}

// deriveServerID 伪从库 ID 缺省派生：etcd 配置中心形态下
// 同 cluster 所有副本共享同一份配置，server_id 必须人工逐节点错开不可持续——
// 按 node.id 做 fnv32a 派生：同节点稳定、异节点几乎必不同（32 位空间）。
// 约束仅「同一台源库上并存的 dump 连接间唯一」：不参与位点（gtid_set/file:pos）、
// 不进 binlog 内容，重启后变化无害。碰撞表现 = MySQL 互踢（可观测，非静默错数据）。
func deriveServerID(nodeID string) uint32 {
	if nodeID == "" {
		nodeID = deriveNodeID()
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(nodeID))
	if id := h.Sum32(); id != 0 {
		return id
	}
	return 1 // fnv32 全零概率 1/2^32，防御 0 非法
}
