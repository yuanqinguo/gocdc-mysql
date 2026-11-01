package pipeline

import (
	"sync"

	"gocdc/internal/model"
)

// Watermark 连续水位（04 §5 权威算法）：
// base = 已被全部 sink 确认的最大连续 seq；位点取 seqPos[base]。
//
//	Worker 成功后 Ack(seq)；Dispatcher 在 XID/Rotate 上 SetPos(seq, pos)。
//	Ack 推进 base 跨越连续已确认段；跨越 XID/Rotate 的 seq 时 lastPos 更新。
//
// 红线：base 是位点推进的唯一来源；lastPos 只会落后于实际投递
// （位点不变式：etcd 位点 ≤ sinks 已收到的最后事件位点，04 §5）。
type Watermark struct {
	mu       sync.Mutex
	base     uint64                    // 连续水位
	pending  map[uint64]bool           // 已 ack 但不连续
	seqPos   map[uint64]model.Position // seq→位点（XID/Rotate 锚点）
	lastPos  model.Position            // base 处的位点（base=0 时零值）
	advanced bool                      // 自上次 Advance 后是否推进（Persister 轮询用）
}

// NewWatermark 构造；startPos 为管线起点（重启恢复后 lastPos 的初值，
// 未越过任何锚点前位点不回退）。
func NewWatermark(startPos model.Position) *Watermark {
	return &Watermark{
		pending: map[uint64]bool{},
		seqPos:  map[uint64]model.Position{},
		lastPos: startPos,
	}
}

// SetPos Dispatcher 记录锚点 seq→位点（XID=事务尾；Rotate=新文件起点）。
func (w *Watermark) SetPos(seq uint64, pos model.Position) {
	w.mu.Lock()
	w.seqPos[seq] = pos
	w.mu.Unlock()
}

// Ack 标记 seq 已确认并尝试推进（04 §5 Ack 语义）。
//
// 幂等（重复 ack 安全）：seq ≤ base 已越过，直接忽略——Worker 对同一
// RowsEvent 的每一行 ack 同一 seq（convertRows 按行 append），若不忽略
// pending 会残留 K-1 条/事件（内存泄漏 + Pending 指标虚高腐蚀洞检测）。
func (w *Watermark) Ack(seq uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if seq <= w.base {
		return
	}
	w.pending[seq] = true
	for w.pending[w.base+1] {
		w.base++
		delete(w.pending, w.base)
		if p, ok := w.seqPos[w.base]; ok {
			w.lastPos = p
			delete(w.seqPos, w.base)
		}
		w.advanced = true
	}
	// 残留锚点无需专门清理：SetPos(seq) 先于该 seq 的 Ack 发生（Dispatcher 分配
	// → Worker 确认），base 跨越任一 seq 时推进循环已就地删除其锚点——全程
	// 均摊 O(1)，无全表扫描。
}

// Advance 返回（并消费）推进状态：ok=true 表示 base 自上次调用后前进了，
// pos 为当前 base 处位点（Persister 周期调用的判据）。
func (w *Watermark) Advance() (safe uint64, pos model.Position, ok bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.advanced {
		return w.base, w.lastPos, false
	}
	w.advanced = false
	return w.base, w.lastPos, true
}

// Current 当前水位与位点快照（强制保存/排障用，不消费推进状态）。
func (w *Watermark) Current() (uint64, model.Position) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.base, w.lastPos
}

// Pending 已确认未连续数（水位洞规模，排障/指标用）。
func (w *Watermark) Pending() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.pending)
}
