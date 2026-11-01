package model

import "time"

// Position binlog 位点：etcd 位点 key 的 value 结构（03 §5.1）。
//
// 红线：位点只能由 Watermark.base 推进后经 Persister 写出，
// 本包不提供任何旁路写位点函数。
type Position struct {
	V       int    `json:"v"`                  // 格式版本，当前 1
	Name    string `json:"name"`               // binlog 文件名
	Pos     uint32 `json:"pos"`                // 文件内偏移
	GTIDSet string `json:"gtid_set,omitempty"` // use_gtid=true 时的 GTID 集合文本

	Seq       uint64    `json:"seq"`         // watermark 连续水位序号（排障用）
	Term      int64     `json:"leader_term"` // 写入时的 leader term（03 §4）
	NodeID    string    `json:"node_id"`     // 写入节点 ID
	UpdatedAt time.Time `json:"updated_at"`  // 最后写入时刻
}
