package sink

import (
	"context"
	"database/sql"
	databaseDriver "database/sql/driver"
	"errors"
	"fmt"
	"net"
	"testing"

	sqldriver "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// 错误分类回归（两类易误判：gRPC 瞬态与驱动失效连接——误判为不可恢复会让
// broker/目标库闪断后整条管线退出）。
func TestIsRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"grpc_unavailable", status.Error(codes.Unavailable, "broker down"), true},
		{"grpc_deadline", status.Error(codes.DeadlineExceeded, "timeout"), true},
		{"grpc_resource", status.Error(codes.ResourceExhausted, "busy"), true},
		{"grpc_perm", status.Error(codes.PermissionDenied, "no auth"), false},
		{"grpc_unauth", status.Error(codes.Unauthenticated, "bad cred"), false},
		{"grpc_invalid", status.Error(codes.InvalidArgument, "bad"), false},
		{"permanent_wins", Permanent(status.Error(codes.Unavailable, "still permanent")), false},
		{"canceled", context.Canceled, false},
		{"driver_invalid_conn", sqldriver.ErrInvalidConn, true},
		{"text_invalid_conn", errors.New("mysql sink: upsert 失败: invalid connection"), true},
		{"conn_refused", errors.New("dial tcp 127.0.0.1:13307: connect: connection refused"), true},
		{"mysql_deadlock", &sqldriver.MySQLError{Number: 1213, Message: "deadlock"}, true},
		{"mysql_syntax", &sqldriver.MySQLError{Number: 1064, Message: "syntax"}, false},
		{"retryable_marker", Retryable(errors.New("顺序组在途冲突")), true},
	}
	for _, c := range cases {
		if got := IsRetryable(c.err); got != c.want {
			t.Fatalf("%s (%v): got %v want %v", c.name, c.err, got, c.want)
		}
	}
	// Retryable 只带可重试标记，不携带 Permanent（worker 分类两轴独立）。
	if IsPermanent(Retryable(errors.New("x"))) {
		t.Fatal("Retryable 不应携带 Permanent 标记")
	}
	if IsRetryable(Permanent(Retryable(errors.New("x")))) {
		t.Fatal("Permanent 优先于 Retryable（不可重试胜出）")
	}
}

// 连接丢失子集分类（stock 块级重连判定用，公网源库流龄强杀场景）：
// 只认「连接已死」，不放大到锁等待/查询超时等业务可重试类。
func TestIsConnLost(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"conn_done", sql.ErrConnDone, true},
		{"conn_done_wrapped", fmt.Errorf("stock: t 扫描块: %w", sql.ErrConnDone), true},
		{"bad_conn", databaseDriver.ErrBadConn, true},
		{"invalid_conn", sqldriver.ErrInvalidConn, true},
		{"text_already_closed", errors.New("sql: connection is already closed"), true},
		{"broken_pipe", errors.New("write tcp 1.2.3.4:3306: write: broken pipe"), true},
		{"net_error", &net.OpError{Op: "read", Err: errors.New("reset")}, true},
		{"lock_timeout", &sqldriver.MySQLError{Number: 1205, Message: "lock wait timeout"}, false},
		{"query_timeout", &sqldriver.MySQLError{Number: 3024, Message: "query timeout"}, false},
		{"syntax", &sqldriver.MySQLError{Number: 1064, Message: "syntax"}, false},
		{"permanent_wins", Permanent(errors.New("sql: connection is already closed")), false},
		{"nil", nil, false},
		{"canceled", context.Canceled, false},
	}
	for _, c := range cases {
		if got := IsConnLost(c.err); got != c.want {
			t.Errorf("%s: IsConnLost = %v, want %v", c.name, got, c.want)
		}
	}
}
