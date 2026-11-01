package position

import (
	"sync"
	"time"
)

// Throttler 保存节流器（03 §5.2：XID 触发默认 3s 节流）。
// 并发安全：Worker/Watermark goroutine 与 Persister 共用。
type Throttler struct {
	interval time.Duration

	mu   sync.Mutex
	last time.Time // 上次实际落盘时刻；零值 = 从未保存（首个 XID 立即允许）
}

// NewThrottler 构造；interval ≤0 视为每次都允许（关闭节流，仅测试用）。
func NewThrottler(interval time.Duration) *Throttler {
	return &Throttler{interval: interval}
}

// Allow 距上次保存是否已满一个窗口（判定通过即视为消费本次配额，
// 由 Persister 在真正发起 Save 前调用）。
func (t *Throttler) Allow(now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.interval <= 0 || now.Sub(t.last) >= t.interval {
		t.last = now
		return true
	}
	return false
}

// Reset 强制保存（rotate/DDL/停机/交接）后重置窗口，避免下一个 XID 又立刻
// 触发一次多余落盘。
func (t *Throttler) Reset(now time.Time) {
	t.mu.Lock()
	t.last = now
	t.mu.Unlock()
}
