package stock

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"gocdc/internal/model"
)

// 锁模式（06 §3 降级矩阵）。
const (
	LockAuto   = "auto"   // 按权限探测：FTWRL → LOCK INSTANCE FOR BACKUP → 报错指明显式 none
	LockFTWRL  = "ftwrl"  // FLUSH TABLES WITH READ LOCK（RELOAD 权限）
	LockBackup = "backup" // LOCK INSTANCE FOR BACKUP（MySQL 8.0 BACKUP_ADMIN，不阻塞 DML）
	LockNone   = "none"   // 显式无锁（近似一致，可能窗口丢失——必须显式配置）
)

// Session 快照会话（06 §2/§4 正确性关键）：持有一组 CONSISTENT SNAPSHOT 连接。
// MySQL 无法跨会话克隆快照——表间并行扫描（06 §7）与「P 时刻视图」只能靠
// FTWRL 锁窗口内克隆连接并各自开事务实现（mydumper 同款手法：全部事务在
// 锁内开启 = 同一锁点视图；sql.Conn 不允许跨 goroutine 并发，一表一连接）：
//
//	ftwrl:  N 连接快照事务全部在锁窗口内开启 = 同一 P 视图（并行安全）
//	backup: 备份锁只挡 DDL 不锚 DML，克隆不增加一致性 → 单连接（串行扫描）
//	none:   显式近似一致 → N 连接各自独立事务（并行，容忍窗口）
//
// ID 进标记，跨会话恢复时 session_id 不匹配 → 整表重扫。
type Session struct {
	ID    string
	Pos   model.Position // 快照对位点 P（06 §2-3）
	db    *sql.DB
	mu    sync.Mutex
	conns []*sql.Conn
}

// Conn 第 i 根快照连接。i 必须落在 [0, len(conns)) 且由调用方保证「一根连接
// 任意时刻至多一个在途扫描」——sql.Conn 不允许跨 goroutine 并发复用（并发共用
// 直接 driver: bad connection）。服务层用槽位 channel 保证独占；严禁按待扫表
// 下标取模取连接（表数>连接数时必然撞到在途扫描的同根连接）。
func (s *Session) Conn(i int) *sql.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conns[i]
}

// RedialSlot 第 i 槽连接被服务端/中间设备掐断时换新连接（公网源库长 TCP 流
// 被网关按流龄强杀场景：公网网关常按几分钟量级的流龄无差别断连）。
// 新连接是「当前视图」而非 P 时刻视图——一致性由收敛不变式保证：全量块是
// 幂等 upsert、位点 P 起的全量增量重放覆盖一切 ≥P 的变更且后到者胜，快照
// 偏新的值会被重放纠正、偏旧则本就是终值（与 lock_mode=none 同构语义，06 §3）。
// 旧连接 ROLLBACK+Close 后驱逐出槽；新连接自动提交（无快照事务可回滚）。
func (s *Session) RedialSlot(ctx context.Context, i int) error {
	c, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("stock: 槽位 %d 重连: %w", i, err)
	}
	s.mu.Lock()
	old := s.conns[i]
	s.conns[i] = c
	s.mu.Unlock()
	if old != nil {
		_, _ = old.ExecContext(context.Background(), "ROLLBACK")
		_ = old.Close()
	}
	return nil
}

// Close 关闭会话：先 ROLLBACK 结束各快照事务再归还连接。连接带着未结束的
// RR 事务回池 = 后续借用方读到 P 时刻的陈旧视图（毒连接），必须显式终结。
func (s *Session) Close() error {
	var firstErr error
	for _, c := range s.conns {
		_, _ = c.ExecContext(context.Background(), "ROLLBACK")
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// OpenSnapshot 打开快照会话（06 §2 步骤 1~5；锁只持几毫秒）：
//
//	锁（按矩阵）→ 快照连接集各自 START TRANSACTION WITH CONSISTENT SNAPSHOT →
//	SHOW MASTER STATUS → P → 解锁。返回后各表扫描走分配到的连接（P 时刻视图）。
func OpenSnapshot(ctx context.Context, db *sql.DB, lockMode string, workers int) (*Session, string, error) {
	if workers < 1 {
		workers = 1
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("stock: 获取快照连接: %w", err)
	}
	sid := newSessionID()

	usedMode, err := acquireLock(ctx, conn, lockMode)
	if err != nil {
		_ = conn.Close()
		return nil, "", err
	}

	conns := []*sql.Conn{conn}
	// 失败清理：放锁 + ROLLBACK 并归还已开快照事务的连接（带事务回池 = 毒连接）
	abort := func(cause error) (*Session, string, error) {
		_, _ = conn.ExecContext(context.Background(), releaseLockSQL(usedMode))
		for _, c := range conns {
			_, _ = c.ExecContext(context.Background(), "ROLLBACK")
			_ = c.Close()
		}
		return nil, usedMode, cause
	}

	// 一致性快照（同一连接：FTWRL 后开启 = 锁点视图）
	if _, err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		return abort(fmt.Errorf("stock: 开启一致性快照: %w", err))
	}

	// 克隆其余快照连接（backup 模式单连接，见 Session 注释）；FTWRL 下全部
	// 在锁窗口内开启事务 = 同一锁点视图，每表一根连接供 worker 并行扫描
	n := workers
	if usedMode == LockBackup {
		n = 1
	}
	for i := 1; i < n; i++ {
		c, cerr := db.Conn(ctx)
		if cerr != nil {
			return abort(fmt.Errorf("stock: 克隆快照连接 %d: %w", i, cerr))
		}
		conns = append(conns, c)
		if _, cerr := c.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); cerr != nil {
			return abort(fmt.Errorf("stock: 快照连接 %d 开启一致性事务: %w", i, cerr))
		}
	}

	// 位点 P（conn0，锁内读取）
	var file string
	var pos uint32
	var i1, i2, g any
	if err := conn.QueryRowContext(ctx, "SHOW BINARY LOG STATUS").Scan(&file, &pos, &i1, &i2, &g); err != nil {
		if err2 := conn.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(&file, &pos, &i1, &i2, &g); err2 != nil {
			return abort(fmt.Errorf("stock: 读取 master 位点: %v / %v", err, err2))
		}
	}

	// 立即解锁（不阻塞业务！06 §2-5）
	if _, err := conn.ExecContext(ctx, releaseLockSQL(usedMode)); err != nil {
		// 解锁失败不影响快照正确性（连接关闭也会释放），记入模式说明
		usedMode += " (unlock_failed)"
	}

	return &Session{
		ID:    sid,
		Pos:   model.Position{Name: file, Pos: pos, GTIDSet: gtids(g)},
		db:    db,
		conns: conns,
	}, usedMode, nil
}

// acquireLock 按模式加锁；auto 依 FTWRL → backup 探测降级，均失败即报错
// （none 必须显式配置，防无感降级——06 §3）。
func acquireLock(ctx context.Context, conn *sql.Conn, mode string) (string, error) {
	switch mode {
	case LockFTWRL:
		return attempt(ctx, conn, "FLUSH TABLES WITH READ LOCK", LockFTWRL)
	case LockBackup:
		return attempt(ctx, conn, "LOCK INSTANCE FOR BACKUP", LockBackup)
	case LockNone:
		return LockNone, nil
	case LockAuto, "":
		if _, err := attempt(ctx, conn, "FLUSH TABLES WITH READ LOCK", LockFTWRL); err == nil {
			return LockFTWRL, nil
		}
		if _, err := attempt(ctx, conn, "LOCK INSTANCE FOR BACKUP", LockBackup); err == nil {
			return LockBackup, nil
		}
		return "", fmt.Errorf("stock: 无快照锁权限（RELOAD/BACKUP_ADMIN 均不可用）；" +
			"如接受近似一致性请显式配置 stock.lock_mode=\"none\"（06 §3，防无感降级）")
	default:
		return "", fmt.Errorf("stock: 未知 lock_mode %q", mode)
	}
}

func attempt(ctx context.Context, conn *sql.Conn, stmt, name string) (string, error) {
	if _, err := conn.ExecContext(ctx, stmt); err != nil {
		return "", err
	}
	return name, nil
}

func releaseLockSQL(mode string) string {
	if mode == LockBackup {
		return "UNLOCK INSTANCE"
	}
	if strings.Contains(mode, "ftwrl") || strings.HasPrefix(mode, LockFTWRL) {
		return "UNLOCK TABLES"
	}
	// none 或 unlock 失败标注：无锁可放
	return "SELECT 1"
}

func gtids(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return ""
}

func newSessionID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// laterPos 位点比较：a 是否晚于 b（文件名比较 + 偏移）。06 §4.1：P2 = max(已记录 P, 当前)。
func laterPos(a, b model.Position) bool {
	if b.Name == "" {
		return true
	}
	if a.Name != b.Name {
		return a.Name > b.Name // binlog 文件名字典序 = 时序
	}
	return a.Pos >= b.Pos
}
