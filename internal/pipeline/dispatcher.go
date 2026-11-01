package pipeline

import (
	"context"
	"hash/fnv"
	"strings"

	"go.uber.org/zap"

	"gocdc/internal/model"
	"gocdc/internal/source"
)

// Dispatcher 消费 source 事件流：seq 分配 + 表级分片路由 + 控制事件（04 §1-§3）。
//
// 分片（04 §2）：shard = fnv1a32(schema+"."+table) % N——同表事件全序（单队列
// FIFO），异表并行；热表倾斜是明示取舍（同行顺序价值 > 单表吞吐）。
//
// 控制事件（04 §3）：
//   - Rotate → PosMark 广播进所有队列（屏障：各 worker 处理到该标记后 ack，
//     水位跨过后 Persister 才可记录跨文件位点）
//   - XID → 不进队：SetPos 锚点 + 立即 ack——水位连续性保证位点只在该事务
//     全部行事件确认后推进（seq 单调 + FIFO ack），并免去 N worker 的全局
//     冲刷同步；行事件的批冲刷由 batch_size/flush_interval 驱动
//   - GTID/Heartbeat → 直通（位点锚点只认 XID/Rotate）
//
// 队列满 → 阻塞 → 反压至 binlog TCP 读（04 §4 天然反压，绝不丢事件）。
type Dispatcher struct {
	rules     *CompiledRules
	excludeSc map[string]bool
	queues    []chan *EventUnit
	log       *zap.Logger
}

// NewDispatcher 构造（queues 由装配层创建，数量 = workers）。
func NewDispatcher(rules *CompiledRules, excludeSchemas []string, queues []chan *EventUnit, log *zap.Logger) *Dispatcher {
	ex := map[string]bool{}
	for _, s := range excludeSchemas {
		ex[strings.ToLower(s)] = true
	}
	if log == nil {
		log = zap.NewNop()
	}
	return &Dispatcher{rules: rules, excludeSc: ex, queues: queues, log: log}
}

// Shard 计算表分片（fnv1a-32，04 §2 默认；导出供测试与排障）。
func Shard(schema, table string, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(strings.ToLower(schema + "." + table)))
	return int(h.Sum32() % uint32(n))
}

// Run 阻塞消费直至 src 关闭或 ctx 取消；返回前关闭所有队列（DrainMark）。
func (d *Dispatcher) Run(ctx context.Context, src <-chan *source.Event, wm *Watermark, nodeID string) error {
	defer d.closeQueues()
	var seq uint64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-src:
			if !ok {
				return nil
			}
			switch ev.Kind {
			case source.KindRow:
				if !d.want(ev.Schema, ev.Table) {
					continue
				}
				seq++
				u := GetUnit()
				*u = EventUnit{
					Seq: seq, Kind: UnitRow, Pos: model.Position{Name: ev.File, Pos: ev.LogPos},
					Schema: ev.Schema, Table: ev.Table,
					Rows: ev.Rows, RowOp: ev.RowOp, Ts: ev.Ts,
				}
				if err := d.send(ctx, Shard(ev.Schema, ev.Table, len(d.queues)), u); err != nil {
					unitPool.Put(u)
					return err
				}
			case source.KindRotate:
				seq++
				// GTIDSet 必须随 rotate 锚点传递：use_gtid 模式下空集合会覆盖
				// 持久化位点（rotate 后重启 validateStart 会拒绝空 gtid_set，
				// 管线致命退出）
				pos := model.Position{Name: ev.File, Pos: ev.LogPos, GTIDSet: ev.GTIDSet}
				wm.SetPos(seq, pos)
				if err := d.broadcast(ctx, &EventUnit{Seq: seq, Kind: UnitRotate, Pos: pos, Ts: ev.Ts}); err != nil {
					return err
				}
			case source.KindXID:
				// 位点锚点：SetPos + 立即 ack（连续性保证先于该 seq 的行全部确认后
				// 位点才推进到事务尾——04 §5）
				seq++
				pos := model.Position{Name: ev.File, Pos: ev.LogPos, GTIDSet: ev.GTIDSet}
				wm.SetPos(seq, pos)
				wm.Ack(seq)
			case source.KindQuery:
				// ROW 模式下非 BEGIN/COMMIT 的 Query 即 DDL（source 已过滤事务语句）。
				// 先解析目标表再过滤与路由（08 §3 barrier = 该表所在 worker 排空
				// 自己队列里该表在途事件后刷新结构）：
				//   - 解析出表 → 过滤（不同步的表直接跳过）→ 路由到表所属分片
				//     （同队列 FIFO 保证 DDL 前的该表行事件全部用旧结构转换完毕，
				//     消除跨 worker 用新结构转旧行像的列数错配竞态）；
				//   - 解析失败 → 该库有任一规则才放行（0 号队列 pause 兜底），
				//     完全未配置的库直接忽略。只认库级规则会让仅表级规则的
				//     库 DDL 被静默丢弃（结构缓存永不刷新，后续行 stale 必炸）。
				dschema, dtable := ddlTarget(ev.Query, ev.Schema)
				if dschema == "" {
					dschema = ev.Schema // 完全解析失败：至少保留会话库（审计记录定位）
				}
				if dtable != "" {
					if !d.want(dschema, dtable) {
						continue // 非同步表：DDL 与行事件同被过滤，无需处理
					}
				} else if !d.schemaConfigured(ev.Schema) {
					continue // 未配置库的复杂 DDL：忽略
				}
				seq++
				pos := model.Position{Name: ev.File, Pos: ev.LogPos, GTIDSet: ev.GTIDSet}
				wm.SetPos(seq, pos)
				u := &EventUnit{Seq: seq, Kind: UnitDDL, Pos: pos, Ts: ev.Ts, Query: ev.Query,
					Schema: dschema, Table: dtable} // 携带解析出的库表（策略匹配/结构刷新依赖）
				shard := 0
				if dtable != "" {
					shard = Shard(dschema, dtable, len(d.queues))
				}
				if err := d.send(ctx, shard, u); err != nil {
					return err
				}
			default:
				// GTID/Heartbeat：位点锚点只认 XID/Rotate（04 §3）
			}
		}
	}
}

// broadcast rotate 屏障：进所有队列（04 §3）。
func (d *Dispatcher) broadcast(ctx context.Context, u *EventUnit) error {
	for i := range d.queues {
		// rotate 单元按队列独立复制（各 worker 独立 ack 同一 seq——水位幂等）
		cp := *u
		if err := d.send(ctx, i, &cp); err != nil {
			return err
		}
	}
	return nil
}

// closeQueues 关闭全部队列（DrainMark；Dispatcher.Run defer 调用）。
func (d *Dispatcher) closeQueues() {
	for i := range d.queues {
		close(d.queues[i])
	}
}

// want 表过滤：库排除 → 无规则 → 放行。
func (d *Dispatcher) want(schema, table string) bool {
	if d.excludeSc[strings.ToLower(schema)] {
		return false
	}
	return d.rules.Match(schema, table) != nil
}

// schemaConfigured 该库是否存在任一规则（库级或任一表级）——DDL 无法定位
// 目标表时的放行判据（放行后 worker pause 人工处置，比静默丢弃安全）。
func (d *Dispatcher) schemaConfigured(schema string) bool {
	if d.want(schema, "") {
		return true // 库级规则存在
	}
	return d.rules.SchemaHasTables(schema)
}

func (d *Dispatcher) send(ctx context.Context, shard int, u *EventUnit) error {
	select {
	case d.queues[shard] <- u:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
