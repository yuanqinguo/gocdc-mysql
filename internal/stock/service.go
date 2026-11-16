package stock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"gocdc/internal/config"
	"gocdc/internal/metrics"
	"gocdc/internal/model"
	"gocdc/internal/pipeline"
	"gocdc/internal/schema"
	"gocdc/internal/sink"
	"gocdc/internal/transform"
)

// Service 全量初始化服务（06 §2 协议编排；仅在 leader 上运行，标记写经 lease 门槛）。
type Service struct {
	db         *sql.DB
	schemaMgr  *schema.Manager
	transform  *transform.Transformer
	sn         sink.Sink
	markers    *MarkerStore
	cfg        config.Stock
	rules      *pipeline.CompiledRules
	met        *metrics.Metrics
	sourceID   string // 指标 source label（多源；空 = 单源/测试）
	log        *zap.Logger
	nodeID     string
	throttleMS int
	schemas    []string // 参与规则的源库名（装配层注入）
}

// New 构造。
func New(db *sql.DB, sm *schema.Manager, tr *transform.Transformer, sn sink.Sink,
	markers *MarkerStore, cfg config.Stock, rules *pipeline.CompiledRules,
	met *metrics.Metrics, sourceID, nodeID string, log *zap.Logger) *Service {
	if log == nil {
		log = zap.NewNop()
	}
	return &Service{
		db: db, schemaMgr: sm, transform: tr, sn: sn, markers: markers,
		cfg: cfg, rules: rules, met: met, sourceID: sourceID, nodeID: nodeID,
		log: log, throttleMS: cfg.ThrottleMS,
	}
}

// Result 全量执行结果。
type Result struct {
	StockPos model.Position // 增量启动位点 = max(所有快照对)（06 §4.1）
	Scanned  int64
	Skipped  int // done 表数
}

// Run 执行全量：对每个 do_stock 表，标记 done 则跳过（重启重跑幂等），
// 否则按协议扫描。返回后装配层以 StockPos 启动增量管线。
func (s *Service) Run(ctx context.Context) (*Result, error) {
	tables, err := s.collectTables(ctx)
	if err != nil {
		return nil, err
	}
	res := &Result{}

	// ① 过滤已完成表（06 §5：done → 跳过）
	var todo []struct {
		db, table string
		rule      *model.Rule
		lastPK    []string
	}
	for _, t := range tables {
		mk, err := s.markers.Get(ctx, t.db, t.table)
		if err != nil {
			return nil, err
		}
		if mk != nil && mk.Status == StatusDone {
			res.Skipped++
			continue
		}
		lastPK := []string(nil)
		if mk != nil && mk.Status != StatusDone && len(mk.LastPK) > 0 {
			// 断点续扫（2026-11-16 review P2-4，原实现无条件丢弃 lastPK 整表重扫）：
			// LastPK 是「已成功投递块」的干净边界（游标推进先于 onProgress 持久化），
			// running/failed 皆可续。跨会话续扫的丢失安全由两点保证：
			//  ① 新会话以当前视图扫余下区间（游标 WHERE pk > LastPK 期间新插入的
			//     更小主键行不在快照中，但其 INSERT 位点 > 各会话 P，必被增量重放）；
			//  ② 增量起点 globalP = 各会话 P 的最小值 ≤ 每个已扫行所在快照视图——
			//     早扫行在 (V1, now] 的变更必被重放，重复由幂等 upsert 吸收。
			// 游标与新主键结构不匹配时 ScanTable 内部降级整表重扫。
			lastPK = mk.LastPK
		}
		todo = append(todo, struct {
			db, table string
			rule      *model.Rule
			lastPK    []string
		}{t.db, t.table, t.rule, lastPK})
	}
	if len(todo) == 0 {
		// 全部 done：增量位点取全局 stock_position（无则回退普通位点引导）
		if p, ok, gerr := s.markers.GlobalPosition(ctx); gerr == nil && ok {
			res.StockPos = p
		}
		return res, nil
	}

	// ② 快照会话（锁毫秒级；P 先行持久化——06 §2-4）
	// 表间并行度 ≤ 待扫表数（快照连接一表一根：sql.Conn 不允许并发复用，
	// 多 worker 共用一根 sess.conn 会直接 driver 报错）
	workers := s.cfg.Workers
	if workers < 1 {
		workers = 4
	}
	if workers > len(todo) {
		workers = len(todo)
	}
	sess, usedMode, err := OpenSnapshot(ctx, s.db, s.cfg.LockMode, workers)
	if err != nil {
		return nil, err
	}
	defer sess.Close()
	s.log.Info("快照会话建立", zap.String("mode", usedMode),
		zap.String("file", sess.Pos.Name), zap.Uint32("pos", sess.Pos.Pos),
		zap.String("session", sess.ID), zap.Int("tables", len(todo)),
		zap.Int("snapshot_conns", workers))

	// P 先行：全局 stock_position = min(已记录, P)（2026-11-16 review P2-4 修正，
	// 原实现取 max）。增量起点必须 ≤ 每个扫描会话的快照视图：跨会话恢复（进程
	// 中断重扫/续扫）时取 max 会把起点推到早会话视图之后——早扫行在 (V1, P2]
	// 窗口内的变更既不在其快照中、又不会被增量重放 = 静默丢数据。取 min 则重放
	// 窗口只会偏大，重复由下游幂等 upsert 吸收（宁可重复、不可丢失）。每会话
	// P 先行持久化保证进程中途死亡也不丢失早会话的 P。
	globalP, ok, _ := s.markers.GlobalPosition(ctx)
	if !ok || laterPos(globalP, sess.Pos) {
		globalP = sess.Pos
	}
	globalP.Seq = 0
	if err := s.markers.PutGlobalPosition(ctx, globalP); err != nil {
		return nil, fmt.Errorf("stock: 位点 P 先行持久化: %w", err)
	}

	// ③ 表间并行扫描（06 §7：表级并行度 stock.workers；连接集内一表一连接）
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	scanner := NewScanner(s.schemaMgr, s.transform, s.sn, s.cfg.ChunkRows)
	// 连接槽位 channel = 限流 + 连接独占一体化：同一时刻第 k 个槽的扫描独占
	// sess.Conn(k)。不能按 todo 下标取连接——Conns 下标取模轮转（i%workers）
	// 会把「仍在扫描中」表的同根连接发给后启动的表，两 goroutine 并发共用
	// sql.Conn 直接 driver: bad connection（待扫表数 > workers 时间歇性触发，
	// 表数 ≤ workers 的场景不会暴露）。
	slots := make(chan int, workers)
	for k := 0; k < workers; k++ {
		slots <- k
	}
	for _, t := range todo {
		wg.Add(1)
		go func(t struct {
			db, table string
			rule      *model.Rule
			lastPK    []string
		}) {
			defer wg.Done()
			slot := <-slots
			defer func() { slots <- slot }()
			if err := s.scanOne(ctx, slot, sess, scanner, t.db, t.table, t.rule, t.lastPK); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(t)
	}
	wg.Wait()
	if firstErr != nil {
		return res, firstErr
	}

	// 汇总扫描行数（done 标记带 RowsDone）
	var scanned int64
	for _, t := range todo {
		if mk, err := s.markers.Get(ctx, t.db, t.table); err == nil && mk != nil {
			scanned += mk.RowsDone
		}
	}
	res.StockPos = globalP
	res.Scanned = scanned
	s.log.Info("全量完成", zap.Int64("rows", res.Scanned), zap.Int("skipped", res.Skipped))
	return res, nil
}

// scanOne 单表扫描 + 标记维护（running → done/failed）。slot = 本表独占的
// 连接槽位（连接经 sess.Conn(slot) 取用；块级连接丢失由 Scanner 内部换连恢复）。
func (s *Service) scanOne(ctx context.Context, slot int, sess *Session, sc *Scanner,
	db, table string, rule *model.Rule, lastPK []string) error {

	// 标记 running（session_id=本会话；跨会话恢复时由此判定整表重扫）
	if err := s.markers.Put(ctx, db, table, &Marker{
		Status: StatusRunning, SessionID: sess.ID, SnapshotP: sess.Pos, LastPK: lastPK,
	}); err != nil {
		return err
	}

	var lastReported int64
	var lastMK *Marker // 最近一次进度标记（failed 写入时保留断点用）
	n, err := sc.ScanTable(ctx, sess, slot, db, table, rule, lastPK, func(mk *Marker) error {
		lastMK = mk
		if s.throttleMS > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(s.throttleMS) * time.Millisecond):
			}
		}
		if s.met != nil {
			s.met.StockRowsTotal.WithLabelValues(s.sourceID, db, table).Add(float64(mk.RowsDone - lastReported))
		}
		lastReported = mk.RowsDone
		return s.markers.Put(ctx, db, table, mk)
	})
	if s.met != nil && err == nil {
		s.met.StockProgressRatio.WithLabelValues(s.sourceID, db, table).Set(1)
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		// 不可重试失败：标记 failed + 告警（绝不静默丢块，06 §4）。断点
		//（LastPK/进度）随标记保留——重启后从断点续扫而非整表重扫
		//（2026-11-16 review P2-4；原实现把进度抹掉导致白扫）。
		failed := &Marker{
			Status: StatusFailed, SessionID: sess.ID, SnapshotP: sess.Pos,
			Error: truncate(err.Error(), 500),
		}
		if lastMK != nil {
			failed.LastPK, failed.ChunkSeq, failed.RowsDone = lastMK.LastPK, lastMK.ChunkSeq, lastMK.RowsDone
		}
		_ = s.markers.Put(ctx, db, table, failed)
		s.log.Error("全量表失败", zap.String("db", db), zap.String("table", table), zap.Error(err))
		return err
	}
	if err := s.markers.Put(ctx, db, table, &Marker{
		Status: StatusDone, SessionID: sess.ID, SnapshotP: sess.Pos, RowsDone: n,
	}); err != nil {
		return err
	}
	s.log.Info("全量表完成", zap.String("db", db), zap.String("table", table), zap.Int64("rows", n))
	return nil
}

// ResetTable restock 运维动作（06 §5）：清标记 → 装配层重跑全量 → 恢复增量。
func (s *Service) ResetTable(ctx context.Context, db, table string) error {
	return s.markers.ResetTable(ctx, db, table)
}

// stockTable 参与全量的表。
type stockTable struct {
	db, table string
	rule      *model.Rule
}

// collectTables 收集 do_stock 表：库级规则展开 information_schema 当前表
// （新表自动纳入，06 §5）；表级规则显式表；exclude_tables 排除。
func (s *Service) collectTables(ctx context.Context) ([]stockTable, error) {
	var out []stockTable
	seen := map[string]bool{}
	add := func(db, table string, rule *model.Rule) {
		k := strings.ToLower(db) + "." + strings.ToLower(table)
		if seen[k] {
			return
		}
		seen[k] = true
		out = append(out, stockTable{db: db, table: table, rule: rule})
	}
	// 库级规则按 information_schema 展开当前表（新表自动纳入，06 §5）
	for _, schemaName := range s.schemaNames() {
		rows, err := s.db.QueryContext(ctx,
			`SELECT TABLE_NAME FROM information_schema.TABLES
			 WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'`, schemaName)
		if err != nil {
			return nil, fmt.Errorf("stock: 展开 %s 表清单: %w", schemaName, err)
		}
		for rows.Next() {
			var tbl string
			if err := rows.Scan(&tbl); err != nil {
				rows.Close()
				return nil, err
			}
			if rule := s.rules.Match(schemaName, tbl); rule != nil && rule.DoStock {
				add(schemaName, tbl, rule)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// schemaNames 参与规则的源库名（去重；从编译前配置传入的 rules 提取不可行——
// 以 Match 的库为口径：由装配层注入）。
func (s *Service) schemaNames() []string { return s.schemas }

// SetSchemas 注入参与规则的库名清单（装配层从 cfg.Rules 归集）。
func (s *Service) SetSchemas(schemas []string) { s.schemas = schemas }

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
