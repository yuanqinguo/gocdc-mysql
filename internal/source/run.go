package source

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"go.uber.org/zap"

	"gocdc/internal/model"
)

// 默认参数（03 §3.2：heartbeat_interval_ms=3000、read_timeout_ms=60000）。
const (
	defaultHeartbeat = 3 * time.Second
	defaultReadTodo  = 60 * time.Second
)

// Run 监督循环（断线监督重启）：
// 从 start 位点起拉流；流错误 → 退避（100ms×2^n 封顶 30s，永不退出）→
// 回放起点 = resume()（有已确认位点则取之，at-least-once 重放）否则内部最后位点。
// 仅 ctx 取消或位点参数非法时返回。
func (s *Source) Run(ctx context.Context, start model.Position, resume ResumeFunc, out chan<- *Event) error {
	pos := start
	if err := s.validateStart(pos); err != nil {
		return err
	}

	backoff := 100 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := s.streamOnce(ctx, pos, out)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.log.Warn("binlog 流中断，将重连", zap.Error(err),
			zap.String("file", s.snapshotPos().Name), zap.Uint32("pos", s.snapshotPos().Pos),
			zap.Duration("backoff", backoff))

		// 会话内已推进到新位点则以此为内部回放基线（resume 回调仍优先）
		if p := s.snapshotPos(); p.Name != "" {
			pos = p
		}
		if resume != nil {
			if p := resume(); p.Name != "" || p.GTIDSet != "" {
				pos = p
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
}

// validateStart 位点合法性（GTID 模式需要 gtid_set；文件模式需要文件名）。
func (s *Source) validateStart(p model.Position) error {
	if s.cfg.UseGTID {
		if p.GTIDSet == "" {
			// use_gtid 切换的存量 file:pos 位点走 Web 迁移（P3）：
			// POST /api/v1/position/migrate-gtid（位点页操作，追平门槛 + 审计）
			return fmt.Errorf("%w: use_gtid=true 需要非空 gtid_set——存量 file:pos 位点请在 Web「同步位点」页执行「迁移位点 → GTID」（或切回 use_gtid=false）", ErrInvalidPosition)
		}
		if _, err := mysql.ParseGTIDSet(s.flavor(), p.GTIDSet); err != nil {
			return fmt.Errorf("%w: gtid_set %q 解析失败: %v", ErrInvalidPosition, p.GTIDSet, err)
		}
		return nil
	}
	if p.Name == "" {
		return fmt.Errorf("%w: 需要非空 binlog 文件名（首启用 SHOW MASTER STATUS / 全量快照协议获得）", ErrInvalidPosition)
	}
	return nil
}

// streamOnce 单次连接会话：构造 syncer → StartSync → 循环出事件。
// 返回时会话必已关闭。错误一律上抛（重连策略在 Run）。
func (s *Source) streamOnce(ctx context.Context, pos model.Position, out chan<- *Event) error {
	syncer := replication.NewBinlogSyncer(s.syncerConfig())
	defer syncer.Close()

	var (
		streamer *replication.BinlogStreamer
		err      error
	)
	if s.cfg.UseGTID {
		gset, perr := mysql.ParseGTIDSet(s.flavor(), pos.GTIDSet)
		if perr != nil {
			return fmt.Errorf("%w: %v", ErrInvalidPosition, perr)
		}
		// 用起始集合初始化 GTID 跟踪：rotate/心跳锚点随 currentGTID() 携带，
		// 否则连接首个 rotate 会把持久化位点的 gtid_set 清空（GTID 模式重启后
		// validateStart 拒绝空集合、管线致命退出）
		s.trackGSet(gset)
		streamer, err = syncer.StartSyncGTID(gset)
	} else {
		streamer, err = syncer.StartSync(mysql.Position{Name: pos.Name, Pos: pos.Pos})
	}
	if err != nil {
		return fmt.Errorf("source: StartSync 失败: %w", err)
	}

	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			return err // 含 ctx.Canceled：Run 层区分
		}
		if err := s.dispatch(ctx, ev, out); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
	}
}

// dispatch 单个 binlog 事件 → 源事件发射 + 内部位点维护。
func (s *Source) dispatch(ctx context.Context, ev *replication.BinlogEvent, out chan<- *Event) error {
	h := ev.Header
	switch e := ev.Event.(type) {
	case *replication.RotateEvent:
		s.mu.Lock()
		s.lastFile = string(e.NextLogName)
		s.lastPos = uint32(e.Position)
		s.mu.Unlock()
		return s.emit(ctx, out, &Event{
			Kind: KindRotate, Artificial: h.Flags&replication.LOG_EVENT_ARTIFICIAL_F != 0,
			Ts: uint64(h.Timestamp), LogPos: uint32(e.Position), File: string(e.NextLogName),
			// rotate 锚点必须携带当前 GTID 集合（use_gtid 下否则被清空）
			GTIDSet: s.currentGTID(),
		})

	case *replication.QueryEvent:
		if s.DebugQueryHook != nil {
			s.DebugQueryHook(string(e.Query), string(e.Schema))
		}
		q := strings.TrimSpace(string(e.Query))
		// 事务边界/保存点语句非业务事件（提交锚点是 XID）。大小写不敏感：
		// BEGIN/COMMIT 由 server 归一为大写，SAVEPOINT 家族是用户原文透传。
		// SAVEPOINT 家族必须拦在 source 层，防止被下游误判为 DDL 且定位不到
		// 表 → pause 冻结全管线直至人工 skip（业务嵌套事务常见 SAVEPOINT）。
		uq := strings.ToUpper(q)
		if uq == "BEGIN" || uq == "COMMIT" || uq == "ROLLBACK" ||
			uq == "SAVEPOINT" || strings.HasPrefix(uq, "SAVEPOINT ") ||
			strings.HasPrefix(uq, "RELEASE SAVEPOINT ") ||
			strings.HasPrefix(uq, "ROLLBACK TO ") || strings.HasPrefix(uq, "XA ") {
			return nil
		}
		s.trackGSet(e.GSet)
		return s.emit(ctx, out, &Event{
			Kind: KindQuery, Ts: uint64(h.Timestamp), LogPos: h.LogPos,
			File: s.snapshotPos().Name, Query: q, GTIDSet: s.currentGTID(),
			Schema: string(e.Schema), // 会话默认库（DDL 路由/规则匹配依赖，勿丢）
		})

	case *replication.XIDEvent:
		s.mu.Lock()
		s.lastPos = h.LogPos
		s.mu.Unlock()
		s.trackGSet(e.GSet)
		return s.emit(ctx, out, &Event{
			Kind: KindXID, Ts: uint64(h.Timestamp), LogPos: h.LogPos,
			File: s.snapshotPos().Name, XID: e.XID, GTIDSet: s.currentGTID(),
		})

	case *replication.GTIDEvent:
		return s.emit(ctx, out, &Event{
			Kind: KindGTID, Ts: uint64(h.Timestamp), LogPos: h.LogPos, File: s.snapshotPos().Name,
		})

	case *replication.HeartbeatEvent:
		return s.emit(ctx, out, &Event{Kind: KindHeartbeat, File: s.snapshotPos().Name})

	case *replication.RowsEvent:
		return s.emit(ctx, out, &Event{
			Kind: KindRow, Ts: uint64(h.Timestamp), LogPos: h.LogPos, File: s.snapshotPos().Name,
			Schema: string(e.Table.Schema), Table: string(e.Table.Table),
			Rows: e, RowOp: e.Type(),
		})

	default:
		return nil // FormatDescription/PreviousGTIDs/TableMap/…：管线无关
	}
}

// emit 阻塞发送（队列满 = 天然反压，04 §4），随 ctx 可中断。
func (s *Source) emit(ctx context.Context, out chan<- *Event, ev *Event) error {
	select {
	case out <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
