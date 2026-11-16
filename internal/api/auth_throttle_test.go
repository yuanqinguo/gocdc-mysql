package api

import (
	"testing"
	"time"
)

// TestAuthThrottle 登录失败限速（2026-11-16 review P3-3）：
// 窗口内累计达上限锁定、锁定期满解锁、跨窗口失败不累计。
func TestAuthThrottle(t *testing.T) {
	th := newAuthThrottle()
	now := time.Now()
	th.now = func() time.Time { return now }

	for i := 0; i < authFailLimit; i++ {
		if th.locked() {
			t.Fatalf("第 %d 次失败前不应锁定", i)
		}
		th.recordFailure()
	}
	if !th.locked() {
		t.Fatal("窗口内失败达上限应锁定")
	}

	now = now.Add(authLockout + time.Second)
	if th.locked() {
		t.Fatal("锁定期满应解锁")
	}

	// 窗口滑动：上一窗口的失败过期后不与新失败累计
	th2 := newAuthThrottle()
	base := time.Now()
	th2.now = func() time.Time { return base }
	for i := 0; i < authFailLimit-1; i++ {
		th2.recordFailure()
	}
	base = base.Add(authFailWindow + time.Second)
	th2.recordFailure() // 过期后的新窗口首败
	if th2.locked() {
		t.Fatal("跨窗口失败不应累计触发锁定")
	}
}
