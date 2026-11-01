// 下游重试退避策略（05 §7）。
package sink

import "time"

// Backoff 指数退避（05 §7：100ms×2^n 封顶 30s）。attempt 从 1 起。
func Backoff(attempt int) time.Duration {
	d := 100 * time.Millisecond << uint(attempt-1)
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}
