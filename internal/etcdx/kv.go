package etcdx

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// KV 是带重试退避的读写包装（03 §5.3：重试 3 次 + 指数退避，瞬时错误不上抛）。
// 每次尝试都有独立的 per-op 超时（默认 3s）；重试判定用 IsRetryable。
//
// 通过 clientv3.Client 的 KV 字段注入假实现可做无容器单测。
type KV struct {
	cli     *clientv3.Client
	timeout time.Duration
	retries int
	backoff func(attempt int) time.Duration
}

// NewKV 构造 KV 包装：per-op 超时 3s、重试 3 次、退避 100ms*2^n 封顶 2s。
func NewKV(cli *clientv3.Client) *KV {
	return &KV{
		cli:     cli,
		timeout: defaultOpTimeout,
		retries: defaultRetries,
		backoff: defaultBackoff,
	}
}

// defaultBackoff 指数退避：attempt 从 1 起，100ms/200ms/400ms…封顶 2s。
func defaultBackoff(attempt int) time.Duration {
	d := 100 * time.Millisecond << uint(attempt-1)
	if d > 2*time.Second {
		d = 2 * time.Second
	}
	return d
}

// Get 读取单个 key（带重试）。
func (k *KV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return retryOp(ctx, k, "Get", func(c context.Context) (*clientv3.GetResponse, error) {
		return k.cli.Get(c, key, opts...)
	})
}

// Put 写入单个 key（带重试）。
func (k *KV) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return retryOp(ctx, k, "Put", func(c context.Context) (*clientv3.PutResponse, error) {
		return k.cli.Put(c, key, val, opts...)
	})
}

// Delete 删除单个 key（带重试）。
func (k *KV) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return retryOp(ctx, k, "Delete", func(c context.Context) (*clientv3.DeleteResponse, error) {
		return k.cli.Delete(c, key, opts...)
	})
}

// Txn 执行事务（带重试）。Txn 构建器是一次性的，build 在每次尝试时重新构造。
// 位点/stock 的 lease 门槛事务走这里。
func (k *KV) Txn(ctx context.Context, build func(clientv3.Txn) clientv3.Txn) (*clientv3.TxnResponse, error) {
	return retryOp(ctx, k, "Txn", func(c context.Context) (*clientv3.TxnResponse, error) {
		return build(k.cli.Txn(c)).Commit()
	})
}

// retryOp 通用重试循环：可重试错误按退避重试至上限，不可重试错误立即上抛；
// 父 ctx 取消立即返回，不再消耗剩余尝试。
func retryOp[T any](ctx context.Context, k *KV, opName string, op func(context.Context) (T, error)) (T, error) {
	var zero T
	var lastErr error
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			d := k.backoff(attempt)
			select {
			case <-ctx.Done():
				return zero, fmt.Errorf("etcd %s 重试等待期间 ctx 结束: %w", opName, ctx.Err())
			case <-time.After(d):
			}
		}
		opCtx, cancel := context.WithTimeout(ctx, k.timeout)
		resp, err := op(opCtx)
		cancel()
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !IsRetryable(err) || ctx.Err() != nil {
			return zero, fmt.Errorf("etcd %s: %w", opName, err)
		}
		if attempt >= k.retries {
			return zero, fmt.Errorf("etcd %s 重试 %d 次后仍失败: %w", opName, k.retries, lastErr)
		}
	}
}
