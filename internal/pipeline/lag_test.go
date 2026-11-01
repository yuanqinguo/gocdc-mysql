package pipeline

import (
	"testing"

	"gocdc/internal/model"
)

// GTID 模式重启后位点停在 rotate 锚点（file:pos 为 dump 流起点），
// 集合覆盖 master executed 必须判 0，不得报字节假滞后。
// 场景：位点 {binlog.000004,4} vs master {binlog.000004,91882}
// 集合同为 1-173 → 字节口径会报 91878 假滞后且永不归零。
func TestComputeLagBytesGTIDRotateAnchor(t *testing.T) {
	cur := model.Position{Name: "binlog.000004", Pos: 4,
		GTIDSet: "c2821573-d425-11f1-8279-6e85c4d29c3f:1-173"}
	master := model.Position{Name: "binlog.000004", Pos: 91882,
		GTIDSet: "c2821573-d425-11f1-8279-6e85c4d29c3f:1-173"}
	if got := computeLagBytes(cur, master, "mysql"); got != 0 {
		t.Fatalf("GTID 集合覆盖应判追平 0，got %v", got)
	}
}

// GTID 真滞后：master 有位点集合未覆盖的新事务 → 落回字节差（非 0 可告警）。
func TestComputeLagBytesGTIDBehind(t *testing.T) {
	cur := model.Position{Name: "binlog.000004", Pos: 4,
		GTIDSet: "uuid:1-100"}
	master := model.Position{Name: "binlog.000004", Pos: 9000,
		GTIDSet: "uuid:1-105"}
	if got := computeLagBytes(cur, master, "mysql"); got != 8996 {
		t.Fatalf("集合未覆盖应回落字节差 8996，got %v", got)
	}
}

// 非 GTID（file:pos）口径：同文件字节差、跨文件视为追平。
func TestComputeLagBytesFilePos(t *testing.T) {
	same := computeLagBytes(model.Position{Name: "b.1", Pos: 100},
		model.Position{Name: "b.1", Pos: 600}, "")
	if same != 500 {
		t.Fatalf("同文件字节差 500，got %v", same)
	}
	cross := computeLagBytes(model.Position{Name: "b.1", Pos: 100},
		model.Position{Name: "b.2", Pos: 4}, "")
	if cross != 0 {
		t.Fatalf("跨文件置 0，got %v", cross)
	}
}
