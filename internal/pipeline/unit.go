// 分发单元定义：UnitKind 类别、EventUnit 结构与行单元对象池；
// 包级 goroutine 拓扑与位点不变式见 doc.go。

package pipeline

import (
	"sync"

	"github.com/go-mysql-org/go-mysql/replication"

	"gocdc/internal/model"
)

// UnitKind 分发单元类别。
type UnitKind uint8

const (
	UnitRow    UnitKind = iota + 1 // 行事件（一个 RowsEvent，Worker 拆行转换）
	UnitRotate                     // binlog 文件切换（同步屏障：drain 后才可跨文件记位点）
	UnitDDL                        // DDL（barrier + ddl_policy 三策略分派）
	UnitXID                        // 事务提交（seq→位点映射锚点）
	UnitDrain                      // 停机排空标记
)

// EventUnit Dispatcher→Worker 的分发单元（转换前，原始）。
type EventUnit struct {
	Seq  uint64 // Dispatcher 分配的全局单调序号
	Kind UnitKind
	Pos  model.Position // 事件位点（rotate = 新文件起点）

	Schema, Table string
	Rows          *replication.RowsEvent // Kind=UnitRow；列信息查 SchemaManager
	RowOp         replication.EnumRowsEventType
	Ts            uint64
	XID           uint64 // Kind=UnitXID

	Query string // Kind=UnitDDL

	TableMeta *model.TableMeta // 转换时用的结构快照（版本化，不可变）
}

// unitPool 行事件单元对象池（04 §4：EventUnit 池化——行单元是唯一热路径，
// rotate/DDL 单元为低频控制事件不走池）。仅回收结构体外壳，Rows 等引用
// 由 GC 处理；Put 前必须已完成转换（不再引用 Rows）。
var unitPool = sync.Pool{New: func() any { return new(EventUnit) }}

// GetUnit 从池取行单元（Dispatcher 专用；其他类别直接构造）。
func GetUnit() *EventUnit { return unitPool.Get().(*EventUnit) }

// PutUnit 归还行单元（Worker 处理完成后调用）。
func PutUnit(u *EventUnit) {
	*u = EventUnit{}
	unitPool.Put(u)
}
