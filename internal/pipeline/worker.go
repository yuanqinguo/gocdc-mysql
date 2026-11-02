package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"go.uber.org/zap"

	"gocdc/internal/metrics"
	"gocdc/internal/model"
	"gocdc/internal/schema"
	"gocdc/internal/sink"
	mysqlsink "gocdc/internal/sink/mysql"
	"gocdc/internal/transform"
)

// Worker 消费分发队列：transform → 攒批 → sink.Send → ack（04 §1）。
// 失败语义（05 §7）：sink 失败 → 上抛 → 管线停（水位停 → 反压；
// 批不清空、绝不跳过事件）。
type Worker struct {
	idx         int // worker 编号（指标/日志）
	sourceID    string // 所属源 sid（DDL 记录/指标/日志溯源，多源 PRD 04 §5）
	queue       <-chan *EventUnit
	rules       *CompiledRules
	schemaMgr   *schema.Manager
	transformer *transform.Transformer
	sn          sink.Sink
	wm          *Watermark
	forceSave   func() // rotate/DDL 时强制位点落盘（Persister 注入）
	ddlHooks    *DDLHooks
	lastEventTs *atomic.Int64
	met         *metrics.Metrics
	log         *zap.Logger

	batchSize    int
	flushTimeout time.Duration

	batch []*model.Event
	seqs  []uint64 // batch 内事件对应 seq（成功后逐个 ack）
}

// NewWorker 构造。forceSave 可 nil（rotate 时无强制落盘——单测用）；
// sourceID 空 = 单源/测试形态（DDL 记录不带 source 字段）。
func NewWorker(queue <-chan *EventUnit, idx int, sourceID string, rules *CompiledRules, sm *schema.Manager,
	tr *transform.Transformer, sn sink.Sink, wm *Watermark, forceSave func(),
	ddlHooks *DDLHooks, lastEventTs *atomic.Int64, met *metrics.Metrics, batchSize int, flushTimeout time.Duration, log *zap.Logger) *Worker {
	if batchSize < 1 {
		batchSize = 500
	}
	if flushTimeout <= 0 {
		flushTimeout = 200 * time.Millisecond
	}
	if log == nil {
		log = zap.NewNop()
	}
	return &Worker{
		idx: idx, sourceID: sourceID, queue: queue, rules: rules, schemaMgr: sm, transformer: tr, sn: sn,
		wm: wm, forceSave: forceSave, ddlHooks: ddlHooks, lastEventTs: lastEventTs, met: met, log: log,
		batchSize: batchSize, flushTimeout: flushTimeout,
	}
}

// DDLHooks DDL 处理外部依赖（装配层注入；nil 字段降级为 pause 行为）。
type DDLHooks struct {
	// DiffTarget 源 vs 目标结构差异 → 白名单 DDL（sink/mysql.DiffTargetTable 闭包）。
	// nil = auto 策略降级为 pause（ddl_sync=false 或非 mysql sink）。
	DiffTarget func(ctx context.Context, schema, table string) (stmts []string, whitelisted bool, summary string, err error)
	// ApplyTarget 执行目标端 DDL（08 §3：60s 超时由实现内兜底）。
	ApplyTarget func(ctx context.Context, stmts []string) error
	// Gate 审批 gate（etcd events/ddl）；nil = pause 直接停管线。
	Gate DDLGate

	// ddlPause 当前等待人工决策的 DDL（nil=无）。运行态覆盖而非生命周期状态：
	// 决策后即恢复，/status 据此在 running 基态上叠加 paused——缺了它管线
	// 暂停期间 /status 仍显 running，前端误报「同步追赶中」。
	ddlPause atomic.Pointer[DDLPauseState]
}

// DDLPauseState 一条等待人工决策的 DDL（/status paused 覆盖语义的数据源）。
type DDLPauseState struct {
	SQL   string
	Since time.Time
}

// Pause 当前 DDL 决策等待（nil=无）。
func (h *DDLHooks) Pause() *DDLPauseState {
	if h == nil {
		return nil
	}
	return h.ddlPause.Load()
}

func (h *DDLHooks) setPause(sql string) {
	h.ddlPause.CompareAndSwap(nil, &DDLPauseState{SQL: sql, Since: time.Now()})
}

// clearPause 撤销等待（仅清自己置入的，防并发多 DDL 互相覆盖）。
func (h *DDLHooks) clearPause(sql string) {
	if p := h.ddlPause.Load(); p != nil && p.SQL == sql {
		h.ddlPause.CompareAndSwap(p, nil)
	}
}

// ErrPaused DDL 命中 pause 策略：管线暂停等待人工决策，无 gate 时停机（08 §2）。
var ErrPaused = errors.New("pipeline: DDL 命中 pause 策略，管线暂停（等待人工决策或无 gate 停机）")

// Run 阻塞消费直至队列关闭或 ctx 取消。
func (w *Worker) Run(ctx context.Context) error {
	timer := time.NewTimer(w.flushTimeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			// 停机：先冲刷已转换批次再退出（04 §8 步骤 2）
			if err := w.flush(ctx); err != nil {
				w.log.Warn("停机冲刷失败（位点将落后，重启重放）", zap.Error(err))
			}
			return ctx.Err()
		case u, ok := <-w.queue:
			if !ok {
				return w.flush(ctx)
			}
			err := w.handle(ctx, u)
			if u.Kind == UnitRow {
				PutUnit(u) // 行单元回收（04 §4 池化）；rotate/DDL 单元为内联构造不回收
			}
			if err != nil {
				return err
			}
		case <-timer.C:
			if err := w.flush(ctx); err != nil {
				return err
			}
			timer.Reset(w.flushTimeout)
		}
	}
}

func (w *Worker) handle(ctx context.Context, u *EventUnit) error {
	switch u.Kind {
	case UnitRow:
		return w.handleRow(ctx, u)
	case UnitRotate:
		// 屏障：冲刷在途批 → 强制位点落盘（跨文件位点此时才可持久化，04 §3）
		if err := w.flush(ctx); err != nil {
			return err
		}
		w.ack(u.Seq)
		if w.forceSave != nil {
			w.forceSave()
		}
		return nil
	case UnitXID:
		// 事务边界：冲刷在途批（事务行尽快投递），XID 自身 seq 在冲刷后确认——
		// 水位跨过该 seq 时位点才推进到事务尾（04 §5）
		if err := w.flush(ctx); err != nil {
			return err
		}
		w.ack(u.Seq)
		return nil
	case UnitDDL:
		return w.handleDDL(ctx, u)
	default:
		return fmt.Errorf("pipeline: 未知单元类型 %d", u.Kind)
	}
}

// handleRow 一个 RowsEvent → 逐行转换入批（04 §1：行级单元；UPDATE 成对像
// 合并为一个事件）。
func (w *Worker) handleRow(ctx context.Context, u *EventUnit) error {
	rule := w.rules.Match(u.Schema, u.Table)
	meta := u.TableMeta
	if meta == nil {
		m, err := w.schemaMgr.Get(ctx, u.Schema, u.Table)
		if err != nil {
			// 表已不存在（DROP 后位点回拨/重放窗口含已删表的历史行事件）：
			// 历史行随表消亡、无同步目标——跳过 + 告警 + ack 放行（必须 ack，
			// 否则永久水位洞）。fatal 会让管线卡死且只能 restock 解。
			// 其余错误（网络等）维持上抛。
			if errors.Is(err, schema.ErrTableNotFound) {
				w.log.Warn("行事件所属表已不存在（重放窗口含已删表），跳过放行",
					zap.String("schema", u.Schema), zap.String("table", u.Table),
					zap.Uint64("seq", u.Seq))
				w.ack(u.Seq)
				return nil
			}
			return fmt.Errorf("pipeline: 加载 %s.%s 结构失败: %w", u.Schema, u.Table, err)
		}
		meta = m
	}

	rows := u.Rows.Rows
	if len(rows) == 0 {
		// 零行 RowsEvent（理论罕见）：seq 不入批就必须在此确认——否则永久
		// 水位洞，位点冻结（04 §5 连续性）
		w.ack(u.Seq)
		return nil
	}
	var err error
	switch u.RowOp {
	case replication.EnumRowsEventTypeInsert:
		err = w.convertRows(u, meta, rule, rows, 1, model.EventInsert)
	case replication.EnumRowsEventTypeUpdate:
		if len(rows)%2 != 0 {
			return fmt.Errorf("pipeline: %s.%s UPDATE 行像不成对: %d", u.Schema, u.Table, len(rows))
		}
		err = w.convertRows(u, meta, rule, rows, 2, model.EventUpdate)
	case replication.EnumRowsEventTypeDelete:
		err = w.convertRows(u, meta, rule, rows, 1, model.EventDelete)
	}
	if err != nil {
		return err
	}
	// 行事件 seq 随批次确认（flush 成功后 w.seqs 统一 ack——位点不得超前于
	// 已投递数据，04 §5；入批即 ack 属位点不变式违规）
	if len(w.batch) >= w.batchSize {
		return w.flush(ctx)
	}
	return nil
}

// convertRows 逐行 transform：step=2 表示每两行为 before/after（UPDATE）。
func (w *Worker) convertRows(u *EventUnit, meta *model.TableMeta, rule *model.Rule,
	rows [][]any, step int, evType model.EventType) error {

	for i := 0; i < len(rows); i += step {
		in := transform.RowInput{
			Meta: meta, Rule: rule,
			Type:      evType,
			EventID:   fmt.Sprintf("%s:%d:%d", u.Pos.Name, u.Pos.Pos, i),
			Ts:        u.Ts,
			XID:       u.XID,
			SourcePos: u.Pos,
		}
		if step == 2 {
			in.Before = rows[i]
			in.After = rows[i+1]
		} else if evType == model.EventInsert {
			in.After = rows[i]
		} else {
			in.Before = rows[i]
		}
		ev, err := w.transformer.Row(in)
		if err != nil {
			var ce *transform.ConvertError
			if errors.As(err, &ce) && ce.IsFailEvent() && w.met != nil {
				// fail_event 转换错误不可重试 → PAUSED + 告警
				w.met.PipelinePaused.WithLabelValues(w.sourceID, u.Schema, u.Table, "convert").Set(1)
			}
			return fmt.Errorf("pipeline: %s.%s 行转换失败: %w", u.Schema, u.Table, err)
		}
		if w.lastEventTs != nil {
			w.lastEventTs.Store(int64(u.Ts))
		}
		if w.met != nil {
			w.met.EventsIn.WithLabelValues(w.sourceID, u.Schema, u.Table, string(evType)).Inc()
		}
		w.batch = append(w.batch, ev)
		w.seqs = append(w.seqs, u.Seq)
	}
	return nil
}

// flush 发送当前批（全部成功才 ack 各 seq）；空批直接返回。
// 攒批时间维度由 Run 的 timer 驱动（flush_interval_ms，03 §3.2）。
//
// 可重试错误（05 §7：锁等待/死锁/断连/超时）按指数退避原地重发当前批
// （批不清空）直到成功或 ctx 取消——瞬时故障管线内自愈，不因单次
// 发送失败停整条管线。不可重试错误立即上抛（管线停 + PAUSED 告警等人工介入）。
// 停机路径（ctx 已取消）用独立 5s 超时 ctx 做最后一次尝试（位点完整度优先）。
func (w *Worker) flush(ctx context.Context) error {
	if len(w.batch) == 0 {
		return nil
	}
	attempt := 0
	for {
		sendCtx := ctx
		if ctx.Err() != nil {
			// 停机 drain：parent ctx 已取消，Send 用取消 ctx 必然立即失败——
			// 换独立短超时 ctx 给最后一次机会（04 §8 步骤 3）
			var cancel context.CancelFunc
			sendCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			err := w.sn.Send(sendCtx, w.batch)
			cancel()
			if err == nil {
				break
			}
			return w.failFlush(err)
		}
		err := w.sn.Send(sendCtx, w.batch)
		if err == nil {
			break
		}
		if !sink.IsRetryable(err) {
			return w.failFlush(err)
		}
		attempt++
		if w.met != nil {
			w.met.SinkRetryTotal.WithLabelValues(w.sourceID, w.sn.Name()).Inc()
		}
		if attempt == 1 || attempt%10 == 0 {
			w.log.Warn("sink 发送失败，退避重试（批保留不清空）",
				zap.Error(err), zap.Int("attempt", attempt),
				zap.Int("batch", len(w.batch)), zap.String("sink", w.sn.Name()))
		}
		select {
		case <-ctx.Done(): // 停机：回到循环顶走停机兜底分支
		case <-time.After(sink.Backoff(attempt)):
		}
	}
	for _, seq := range w.seqs {
		w.ack(seq)
	}
	w.batch = w.batch[:0]
	w.seqs = w.seqs[:0]
	return nil
}

// failFlush 不可重试失败的统一出口：PAUSED 指标 + 包装上抛（管线停，
// 05 §7 绝不静默跳过；人工修复后重启进程从已存位点重放恢复）。
func (w *Worker) failFlush(err error) error {
	if w.met != nil && len(w.batch) > 0 {
		first := w.batch[0]
		w.met.PipelinePaused.WithLabelValues(w.sourceID, first.Database, first.Table, "sink").Set(1)
	}
	return fmt.Errorf("pipeline: sink 发送失败（批 %d 事件，不可恢复）: %w", len(w.batch), err)
}

func (w *Worker) ack(seq uint64) { w.wm.Ack(seq) }

// handleDDL barrier + 三策略（08 §2/§3）：
//  1. 冲刷该表在途批（barrier：DDL 前事件用旧 schema 且已投递确认）；
//  2. 刷新源 schema（源端 DDL 已发生）；
//  3. 按规则 ddl_policy 分派：ignore / auto（白名单）/ pause（审批 gate）。
//
// barrier 语义：DDL seq 在 barrier 完成前不 ack——水位不越过 DDL 位点（08 §3）。
func (w *Worker) handleDDL(ctx context.Context, u *EventUnit) error {
	if err := w.flush(ctx); err != nil {
		return err
	}

	// DDL 目标表定位：QueryEvent 不携带表名，从语句解析（08 §3 barrier 的
	// 表定位）；解析失败 → pause（不猜，安全兜底）
	schemaName, tableName := u.Schema, u.Table
	if tableName == "" {
		schemaName, tableName = ddlTarget(u.Query, u.Schema)
		if tableName == "" {
			w.log.Warn("DDL 无法定位目标表（复杂形态），pause 人工处理", zap.String("sql", u.Query))
			// 记录 ID 仍按 binlog 坐标生成（重放稳定）：空 ID 会让断点续跑
			// 幂等失效，且同类事件互覆同一条 etcd 记录
			return w.pauseOnDDL(ctx, u, DDLRecord{
				ID:       DDLRecordID(u.Schema, "", u.Pos.Name, u.Pos.Pos),
				Schema:   u.Schema,
				SQL:      u.Query,
				Position: fmt.Sprintf("%s:%d", u.Pos.Name, u.Pos.Pos),
				Source:   w.sourceID,
			}, "", "", "无法定位目标表")
		}
	}

	rule := w.rules.Match(schemaName, tableName)
	if rule == nil {
		// 不同步范围的表（exclude_tables 命中/未配置）：DDL 与该表行事件
		// 同被过滤，无需刷新结构、无需审批——审计后跳过。按默认 pause
		// 挂起会冻结全局位点（一张被排除的临时表 TRUNCATE 即停整条管线）。
		w.log.Info("DDL 属非同步表，忽略（审计）",
			zap.String("schema", schemaName), zap.String("table", tableName),
			zap.String("sql", u.Query))
		w.recordDDL(ctx, DDLRecord{
			ID:     DDLRecordID(schemaName, tableName, u.Pos.Name, u.Pos.Pos),
			Schema: schemaName, Table: tableName, SQL: u.Query,
			Position: fmt.Sprintf("%s:%d", u.Pos.Name, u.Pos.Pos), Policy: "ignored",
			Source: w.sourceID,
		}, DecisionSkip, "非同步范围表")
		w.ack(u.Seq)
		return nil
	}
	policy := "pause"
	if rule.DDLPolicy != "" {
		policy = rule.DDLPolicy
	}

	if _, rerr := w.schemaMgr.Refresh(ctx, schemaName, tableName); rerr != nil {
		// 表被 DROP 等：结构刷新失败按 pause 人工介入，绝不静默跳过
		w.log.Warn("DDL 后源表结构刷新失败", zap.Error(rerr),
			zap.String("schema", schemaName), zap.String("table", tableName))
	}

	rec := DDLRecord{
		ID:     DDLRecordID(schemaName, tableName, u.Pos.Name, u.Pos.Pos),
		Schema: schemaName, Table: tableName,
		SQL: u.Query, Position: fmt.Sprintf("%s:%d", u.Pos.Name, u.Pos.Pos),
		Policy: policy, Source: w.sourceID,
	}

	switch policy {
	case "ignore":
		w.log.Warn("DDL ignore（审计）",
			zap.String("schema", schemaName), zap.String("table", tableName), zap.String("sql", u.Query))
		w.recordDDL(ctx, rec, DecisionSynced, "ignore 策略自动跳过")
		if w.forceSave != nil {
			w.forceSave()
		}
		w.ack(u.Seq)
		return nil

	case "auto":
		hooks := w.ddlHooks
		if hooks == nil || hooks.DiffTarget == nil || hooks.ApplyTarget == nil {
			w.log.Warn("ddl_policy=auto 但 ddl_sync 未启用（targets.mysql.ddl_sync=false）→ 降级 pause")
			return w.pauseOnDDL(ctx, u, rec, schemaName, tableName, "auto 降级：ddl_sync 未启用")
		}
		stmts, ok, summary, err := hooks.DiffTarget(ctx, schemaName, tableName)
		if err != nil {
			return fmt.Errorf("pipeline: DDL 差异计算失败 %s.%s: %w", schemaName, tableName, err)
		}
		if !ok {
			return w.pauseOnDDL(ctx, u, rec, schemaName, tableName, "白名单外差异: "+summary)
		}
		if err := hooks.ApplyTarget(ctx, stmts); err != nil {
			return fmt.Errorf("pipeline: 目标 DDL 执行失败 %s.%s（pause 人工处理）: %w", schemaName, tableName, err)
		}
		_, ok2, summary2, derr := hooks.DiffTarget(ctx, schemaName, tableName)
		if derr != nil || !ok2 {
			return fmt.Errorf("pipeline: 目标 DDL 执行后复验不一致 %s.%s: %v %s", schemaName, tableName, derr, summary2)
		}
		w.recordDDL(ctx, rec, DecisionSynced, "auto 白名单同步: "+summary)
		w.log.Info("DDL 自动同步完成",
			zap.String("schema", schemaName), zap.String("table", tableName),
			zap.Int("stmts", len(stmts)), zap.String("summary", summary))
		if w.forceSave != nil {
			w.forceSave()
		}
		w.ack(u.Seq)
		return nil

	default:
		return w.pauseOnDDL(ctx, u, rec, schemaName, tableName, "ddl_policy=pause")
	}
}

func (w *Worker) recordDDL(ctx context.Context, rec DDLRecord, status, note string) {
	if w.ddlHooks == nil || w.ddlHooks.Gate == nil {
		return
	}
	rec.Status, rec.Note = status, note
	rec.DecidedAt = time.Now()
	if err := w.ddlHooks.Gate.Record(ctx, rec); err != nil {
		w.log.Warn("DDL 记录写入失败", zap.Error(err), zap.String("id", rec.ID))
	}
}

// pauseOnDDL 暂停等待人工决策（08 §5 A/C 流程）：位点固化 → 写 pending →
// 阻塞等 approve/skip。水位洞使位点冻结（等待期人工介入语义）。
// Gate 未装配（兼容路径）→ 停管线。
func (w *Worker) pauseOnDDL(ctx context.Context, u *EventUnit, rec DDLRecord, schemaName, tableName, reason string) error {
	if w.forceSave != nil {
		w.forceSave()
	}
	if w.met != nil {
		w.met.PipelinePaused.WithLabelValues(w.sourceID, schemaName, tableName, "ddl").Set(1)
	}
	if w.ddlHooks == nil || w.ddlHooks.Gate == nil {
		w.log.Warn("DDL 暂停（无审批 gate，管线停止）",
			zap.String("schema", u.Schema), zap.String("table", u.Table),
			zap.String("sql", u.Query), zap.String("reason", reason))
		return fmt.Errorf("%w: %s.%s %s", ErrPaused, u.Schema, u.Table, u.Query)
	}
	// 等待期可见性：/status 在 running 基态上叠加 paused（决策返回即清）
	w.ddlHooks.setPause(u.Query)
	defer w.ddlHooks.clearPause(u.Query)
	rec.Note = reason
	// 断点续跑（08 §5 幂等）：先查既有决策——重启重放到同一 DDL 时不得把
	// 已决记录覆盖回 pending（否则决策丢失、worker 永久等待）
	var dec string
	if settled, ok, serr := w.ddlHooks.Gate.Settled(ctx, rec.ID); serr == nil && ok {
		dec = settled
		w.log.Info("DDL 命中既有决策（断点续跑）",
			zap.String("id", rec.ID), zap.String("decision", dec))
	} else {
		if err := w.ddlHooks.Gate.Record(ctx, rec); err != nil {
			return fmt.Errorf("pipeline: DDL 挂起记录写入失败: %w", err)
		}
		w.log.Warn("DDL 暂停等待人工决策（approve/skip）",
			zap.String("id", rec.ID), zap.String("schema", schemaName),
			zap.String("table", tableName), zap.String("sql", u.Query), zap.String("reason", reason))
		var err error
		dec, err = w.ddlHooks.Gate.WaitDecision(ctx, rec.ID)
		if err != nil {
			return fmt.Errorf("pipeline: 等待 DDL 决策: %w", err)
		}
	}
	switch dec {
	case DecisionApprove:
		hooks := w.ddlHooks
		if hooks.DiffTarget == nil || hooks.ApplyTarget == nil {
			// 流程 B：人工已在目标端自行修改 → 复验由后续 diff 兜底
			w.log.Info("DDL 决策 approve（无自动执行器，按新 schema 继续）", zap.String("id", rec.ID))
		} else {
			stmts, ok, summary, derr := hooks.DiffTarget(ctx, schemaName, tableName)
			if derr != nil {
				// 源表已不存在（审批挂起期间被后续 DDL 删除/改名）→ 本事件过期，
				// 按 skip 放行继续处理后续事件；fatal 会让管线卡死在已不可能
				// 通过复验的状态
				if errors.Is(derr, mysqlsink.ErrSourceTableGone) {
					w.log.Warn("DDL 已过期（源表已删除/改名），按 skip 放行",
						zap.String("id", rec.ID), zap.Error(derr))
					w.recordDDL(ctx, rec, DecisionSkip, "源表已不存在，DDL 过期放行")
					if w.forceSave != nil {
						w.forceSave()
					}
					w.ack(u.Seq)
					return nil
				}
				return fmt.Errorf("pipeline: DDL 决策后差异计算失败: %w", derr)
			}
			if !ok {
				return fmt.Errorf("pipeline: DDL approve 后仍有白名单外差异 %s.%s: %s", schemaName, tableName, summary)
			}
			if len(stmts) > 0 {
				if aerr := hooks.ApplyTarget(ctx, stmts); aerr != nil {
					return fmt.Errorf("pipeline: DDL approve 执行失败 %s.%s: %w", schemaName, tableName, aerr)
				}
			}
			w.log.Info("DDL approve 执行完成", zap.String("id", rec.ID), zap.String("summary", summary))
		}
	case DecisionSkip, DecisionSynced:
		// synced = 该 DDL 曾被 auto 策略同步过（策略改 pause 后重放）——
		// 幂等跳过等待，按新 schema 继续
		w.log.Warn("DDL 决策 skip（按新 schema 继续，审计留痕）", zap.String("id", rec.ID), zap.String("decision", dec))
	default:
		return fmt.Errorf("pipeline: 未支持的 DDL 决策 %q", dec)
	}
	w.recordDDL(ctx, rec, dec, "人工决策")
	if w.forceSave != nil {
		w.forceSave()
	}
	w.ack(u.Seq) // barrier 完成：水位可越过 DDL 位点（08 §3-7）
	return nil
}
