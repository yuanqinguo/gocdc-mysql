// 下游投递错误分类。包概述、红线与失败语义见 doc.go。

package sink

import (
	"context"
	"database/sql"
	databaseDriver "database/sql/driver"
	"errors"
	"net"
	"strings"

	sqldriver "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrPermanent 不可重试错误标记（SQL 语法/结构不符/认证拒绝/消息超限等）。
// 上层 pipeline 据此把规则置 PAUSED + 告警，位点停推等人工介入。
var ErrPermanent = errors.New("sink: 不可重试错误")

// ErrRetryable 可重试错误标记（瞬态冲突/结果未知等无法按错误类型自动识别、
// 但退避重试即可收敛的错误，如 RocketMQ 顺序组在途冲突）。
var ErrRetryable = errors.New("sink: 可重试错误")

// Permanent 把 err 包装为不可重试错误（已是 permanent 则原样返回）。
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrPermanent) {
		return err
	}
	return errors.Join(ErrPermanent, err)
}

// Retryable 把 err 标记为可重试（IsRetryable 恒真；不覆盖原有分类语义）。
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(ErrRetryable, err)
}

// IsPermanent 判断错误是否携带不可重试标记。
func IsPermanent(err error) bool { return errors.Is(err, ErrPermanent) }

// MySQL 可重试错误码（网络瞬态/锁等待/连接丢失）。
// 不在清单内的 MySQL 错误一律视为不可重试——宁可 PAUSED 人工介入，
// 不可盲目重试放大问题（05 §7：绝不静默跳过）。
var retryableMySQLCodes = map[uint16]bool{
	1205: true, // ER_LOCK_WAIT_TIMEOUT：锁等待超时（等锁可能成功）
	1213: true, // ER_LOCK_DEADLOCK：死锁回退重试
	2006: true, // CR_SERVER_GONE_ERROR：服务端断连
	2013: true, // CR_SERVER_LOST：查询期间失去连接（结果未知，按可重试重发）
	2055: true, // CR_SERVER_LOST_EXTENDED：连接意外断开
	1613: true, // ER_XA_RBTIMEOUT 等瞬态——保留扩展位
	3024: true, // ER_QUERY_TIMEOUT：查询超时（可重试）
}

// IsRetryable 错误分类：错误链或驱动错误码判定。
// context.Canceled 不重试（主动停机）；DeadlineExceeded 按瞬态重试。
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrPermanent) || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, ErrRetryable) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, databaseDriver.ErrBadConn) {
		return true
	}
	if errors.Is(err, sql.ErrConnDone) { // "sql: connection is already closed"（sql.Conn 已被判定死亡）
		return true
	}
	// go-sql-driver 连接失效标记（池内连接在目标库网络闪断后必现）：无 MySQL
	// 错误码、不实现 net.Error，不识别会把目标库瞬断判成不可恢复、整条管线退出
	if errors.Is(err, sqldriver.ErrInvalidConn) {
		return true
	}
	var myErr *sqldriver.MySQLError
	if errors.As(err, &myErr) {
		return retryableMySQLCodes[myErr.Number]
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true // 网络层错误一律瞬态（超时/拒连/重置）
	}
	// gRPC 状态错误（RocketMQ v5 等 gRPC 客户端）：status 错误不实现 net.Error，
	// 不专门识别会把 broker 瞬断判成不可恢复、整条管线退出。按 gRPC 语义分类。
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted,
			codes.Aborted, codes.Internal, codes.Unknown:
			return true
		case codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated,
			codes.NotFound, codes.FailedPrecondition, codes.Unimplemented:
			return false
		}
	}
	// 兜底：驱动/未知错误的文本特征（部分错误不实现 net.Error）
	msg := err.Error()
	for _, marker := range []string{
		"connection refused", "connection reset", "broken pipe",
		"driver: bad connection", "invalid connection", "EOF",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

// IsConnLost 连接丢失类错误（IsRetryable 的连接子集）：供全量扫描块级重连
// 判定——只对「连接已死」换连接重试，不放大到锁等待/查询超时等业务可重试类
// （公网源库长连接会被网关按流龄强杀，需换连接重试）。
func IsConnLost(err error) bool {
	if err == nil || IsPermanent(err) {
		return false
	}
	if errors.Is(err, databaseDriver.ErrBadConn) ||
		errors.Is(err, sqldriver.ErrInvalidConn) ||
		errors.Is(err, sql.ErrConnDone) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	msg := err.Error()
	for _, marker := range []string{
		"connection refused", "connection reset", "broken pipe",
		"driver: bad connection", "invalid connection", "connection is already closed", "EOF",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}
