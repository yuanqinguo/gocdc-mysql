package etcdx

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// NewSession 创建 lease session（TTL 秒，clientv3 内置 KeepAlive，TTL/3 周期续约）。
// 创建失败按指数退避重试至上限——03 §4.1：瞬时故障不允许直接放弃；
// 完整的「永不退出重选循环」属于 elector，这里只做单次获取的加固。
func NewSession(ctx context.Context, cli *clientv3.Client, ttl int64) (*concurrency.Session, error) {
	var lastErr error
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			d := defaultBackoff(attempt)
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("创建 etcd session 等待期间 ctx 结束: %w", ctx.Err())
			case <-time.After(d):
			}
		}
		sess, err := concurrency.NewSession(cli, concurrency.WithTTL(int(ttl)), concurrency.WithContext(ctx))
		if err == nil {
			return sess, nil
		}
		lastErr = err
		if !IsRetryable(err) || ctx.Err() != nil {
			return nil, fmt.Errorf("创建 etcd session(ttl=%d): %w", ttl, err)
		}
		if attempt >= defaultRetries {
			return nil, fmt.Errorf("创建 etcd session 重试 %d 次后仍失败: %w", defaultRetries, lastErr)
		}
	}
}
