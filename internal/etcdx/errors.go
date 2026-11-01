package etcdx

import (
	"context"
	"errors"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// IsRetryable 判定 etcd 错误是否可重试：
//   - 可重试：网络瞬态/连接失败、etcd unavailable、context deadline（瞬态超时）
//   - 不可重试：认证/授权拒绝、调用方主动取消
//
// 未识别的错误默认按可重试处理（网络故障大多表现为非分类错误），
// 由调用方的重试上限兜底。
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// 认证/授权类拒绝：重试无意义，直接上抛
	if errors.Is(err, rpctypes.ErrPermissionDenied) ||
		errors.Is(err, rpctypes.ErrAuthFailed) ||
		errors.Is(err, rpctypes.ErrAuthOldRevision) {
		return false
	}
	return true
}
