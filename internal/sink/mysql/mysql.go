package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"gocdc/internal/metrics"
	"gocdc/internal/model"
	"gocdc/internal/sink"
)

// Options 连接与切块参数（config.Targets.MySQL 映射，05 §5.3）。
type Options struct {
	Addr            string
	User            string
	Password        string
	Charset         string // 默认 utf8mb4
	MaxOpenConns    int    // 必填 ≥1（配置校验强制）
	MaxIdleConns    int    // 0 = 5
	ConnMaxLifetime time.Duration
	TxChunkRows     int    // 0 = 500：单语句最大行数
	Metrics         *metrics.Metrics
	SourceID        string // 指标 source label + envelope source 字段（多源；空 = 单源/测试）

	// TargetDB 目标库解析（05 §5.1.1）：源库/表 → 目标库（可配默认库+规则覆盖，
	// 表名不可改）。nil 或返回空 = 与源库同名。
	TargetDB func(schema, table string) string
}

// Sink MySQL 直写（05 §5）。Send 全部成功才返回 nil（05 §1 同步语义）。
type Sink struct {
	db        *sql.DB
	chunkRows int
	metrics   *metrics.Metrics
	sourceID  string // 指标 source label（envelope 不经 mysql sink——SQL 无消息封皮）
	targetDB  func(schema, table string) string
}

// New 打开连接池（DSN：interpolateParams + 5s 三超时，05 §5.3）。
func New(opts Options) (*Sink, error) {
	if opts.MaxOpenConns < 1 {
		return nil, fmt.Errorf("mysql sink: max_open_conns 必须 ≥1（0=无上限会打爆目标库）")
	}
	charset := opts.Charset
	if charset == "" {
		charset = "utf8mb4"
	}
	chunk := opts.TxChunkRows
	if chunk <= 0 {
		chunk = 500
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=%s&interpolateParams=true&timeout=5s&readTimeout=5s&writeTimeout=5s",
		opts.User, opts.Password, opts.Addr, charset)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql sink: 打开连接池: %w", err)
	}
	db.SetMaxOpenConns(opts.MaxOpenConns)
	if opts.MaxIdleConns > 0 {
		db.SetMaxIdleConns(opts.MaxIdleConns)
	}
	if opts.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(opts.ConnMaxLifetime)
	}
	return &Sink{db: db, chunkRows: chunk, metrics: opts.Metrics, sourceID: opts.SourceID, targetDB: opts.TargetDB}, nil
}

// resolveDB 目标库解析（规则 target_database > 全局默认 > 源库同名）。
func (s *Sink) resolveDB(schema, table string) string {
	if s.targetDB != nil {
		if t := s.targetDB(schema, table); t != "" {
			return t
		}
	}
	return schema
}

func (s *Sink) Name() string { return "mysql" }

// Ping 存活检查（SELECT 1）。
func (s *Sink) Ping(ctx context.Context) error {
	var one int
	if err := s.db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		return fmt.Errorf("mysql sink: ping: %w", err)
	}
	return nil
}

// Close 关闭连接池（等待在途返回）。
func (s *Sink) Close(_ context.Context) error { return s.db.Close() }

// DB 暴露底层连接池（结构核对/运维诊断用；业务写入一律走 Send）。
func (s *Sink) DB() *sql.DB { return s.db }

// runKind 游程类型。
type runKind uint8

const (
	runUpsert runKind = iota + 1
	runDelete
	runPkChange // 同事务 delete(旧) + upsert(新)
)

// run 一条（组）语句：游程内同表同类型操作合并，chunkRows 上限在建段时保证。
type run struct {
	kind                      runKind
	db                        string
	table                     string
	cols                      []string // upsert/pkChange：目标列序
	rows                      [][]any  // upsert/pkChange：新行值
	pkCols                    []string // delete/pkChange：主键列序
	olds                      [][]any  // delete/pkChange：旧主键元组
	nInsert, nUpdate, nDelete int      // 归属事件数（按类型计指标）
}

// Send 批量写入（05 §5.3）：
//  1. 按批内顺序游程分组（同表同类型连续段合并；PK 变更打断游程生成成对事务）；
//  2. 逐段执行——单语句段直接执行（语句自身原子），PK 变更段显式事务；
//  3. 任一段失败立即上抛（绝不跳过），由上层按错误分类重试/熔断。
func (s *Sink) Send(ctx context.Context, batch []*model.Event) error {
	if len(batch) == 0 {
		return nil
	}
	var runs []run
	cur := (*run)(nil)

	flush := func() {
		if cur != nil {
			runs = append(runs, *cur)
			cur = nil
		}
	}
	appendRow := func(k runKind, ev *model.Event, cols []string, row []any, pkCols []string, old []any) {
		db, table := s.resolveDB(ev.Database, ev.Table), ev.Table
		if cur == nil || cur.kind != k || cur.db != db || cur.table != table ||
			!equalStrings(cur.cols, cols) || !equalStrings(cur.pkCols, pkCols) ||
			(k == runUpsert && len(cur.rows) >= s.chunkRows) ||
			(k == runDelete && len(cur.olds) >= s.chunkRows) ||
			(k == runPkChange && len(cur.rows) >= s.chunkRows) {
			flush()
			cur = &run{kind: k, db: db, table: table, cols: cols, pkCols: pkCols}
		}
		switch ev.Type {
		case model.EventInsert:
			cur.nInsert++
		case model.EventUpdate:
			cur.nUpdate++
		case model.EventDelete:
			cur.nDelete++
		}
		switch k {
		case runUpsert:
			cur.rows = append(cur.rows, row)
		case runDelete:
			cur.olds = append(cur.olds, old)
		case runPkChange:
			cur.olds = append(cur.olds, old)
			cur.rows = append(cur.rows, row)
		}
	}

	for _, ev := range batch {
		switch ev.Type {
		case model.EventInsert:
			cols, row, err := upsertRow(ev)
			if err != nil {
				return err
			}
			appendRow(runUpsert, ev, cols, row, nil, nil)
		case model.EventDelete:
			pkCols, old, err := deleteTuple(ev, ev.Pk)
			if err != nil {
				return err
			}
			appendRow(runDelete, ev, nil, nil, pkCols, old)
		case model.EventUpdate:
			if !pkChanged(ev) {
				cols, row, err := upsertRow(ev)
				if err != nil {
					return err
				}
				appendRow(runUpsert, ev, cols, row, nil, nil)
				continue
			}
			pkCols, old, err := deleteTuple(ev, ev.Before)
			if err != nil {
				return err
			}
			cols, row, err := upsertRow(ev)
			if err != nil {
				return err
			}
			// 主键变更：旧删新插必须成对同事务（05 §5.2）——打断游程单独成段
			flush()
			appendRow(runPkChange, ev, cols, row, pkCols, old)
		default:
			return sink.Permanent(fmt.Errorf("mysql sink: %s.%s 收到不支持的事件类型 %s",
				ev.Database, ev.Table, ev.Type))
		}
	}
	flush()

	for i := range runs {
		if err := s.execRun(ctx, &runs[i]); err != nil {
			return err
		}
	}
	return nil
}

// execRun 执行一个游程段；PK 变更段 = 显式事务（05 §5.2 同事务语义）。
func (s *Sink) execRun(ctx context.Context, r *run) error {
	start := time.Now()
	var err error
	switch r.kind {
	case runUpsert:
		_, err = s.db.ExecContext(ctx, buildUpsert(r.db, r.table, r.cols, len(r.rows)), flatten(r.rows)...)
	case runDelete:
		_, err = s.db.ExecContext(ctx, buildDelete(r.db, r.table, r.pkCols, len(r.olds)), flatten(r.olds)...)
	case runPkChange:
		err = s.execPkChange(ctx, r)
	}
	if s.metrics != nil {
		s.metrics.SinkBatchDuration.WithLabelValues(s.sourceID, s.Name(), r.db, r.table).
			Observe(time.Since(start).Seconds())
	}
	if err != nil {
		return classifyExec(r, err)
	}
	if s.metrics != nil {
		// 游程段成功 = 段内事件全部投递；按事件类型分标签累计
		s.metrics.EventsOut.WithLabelValues(s.sourceID, r.db, r.table, "insert", s.Name()).Add(float64(r.nInsert))
		s.metrics.EventsOut.WithLabelValues(s.sourceID, r.db, r.table, "update", s.Name()).Add(float64(r.nUpdate))
		s.metrics.EventsOut.WithLabelValues(s.sourceID, r.db, r.table, "delete", s.Name()).Add(float64(r.nDelete))
	}
	return nil
}

// execPkChange delete(旧) + upsert(新) 同事务。
func (s *Sink) execPkChange(ctx context.Context, r *run) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mysql sink: %s.%s 主键变更事务开启失败: %w", r.db, r.table, err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, buildDelete(r.db, r.table, r.pkCols, len(r.olds)), flatten(r.olds)...); err != nil {
		return classifyExec(r, fmt.Errorf("mysql sink: %s.%s 主键变更删除旧行: %w", r.db, r.table, err))
	}
	if _, err := tx.ExecContext(ctx, buildUpsert(r.db, r.table, r.cols, len(r.rows)), flatten(r.rows)...); err != nil {
		return classifyExec(r, fmt.Errorf("mysql sink: %s.%s 主键变更写入新行: %w", r.db, r.table, err))
	}
	if err := tx.Commit(); err != nil {
		return classifyExec(r, fmt.Errorf("mysql sink: %s.%s 主键变更提交: %w", r.db, r.table, err))
	}
	return nil
}

// String 游程类型名（日志/错误）。
func (k runKind) String() string {
	switch k {
	case runUpsert:
		return "upsert"
	case runDelete:
		return "delete"
	case runPkChange:
		return "pk_change"
	}
	return "unknown"
}

// classifyExec 错误分类：可重试原样上抛，不可重试包 ErrPermanent
// 供 pipeline PAUSED（05 §7：绝不静默跳过）。
func classifyExec(r *run, err error) error {
	if err == nil {
		return nil
	}
	if sink.IsRetryable(err) {
		return fmt.Errorf("mysql sink: %s.%s %s 失败: %w", r.db, r.table, r.kind, err)
	}
	return sink.Permanent(fmt.Errorf("mysql sink: %s.%s %s 失败: %w", r.db, r.table, r.kind, err))
}

// upsertRow 从事件后像提取目标列序与值。
func upsertRow(ev *model.Event) ([]string, []any, error) {
	if ev.After == nil {
		return nil, nil, sink.Permanent(fmt.Errorf("mysql sink: %s.%s %s 事件缺后像",
			ev.Database, ev.Table, ev.Type))
	}
	cols := sortedKeys(ev.After)
	row := make([]any, 0, len(cols))
	for _, c := range cols {
		row = append(row, ev.After[c])
	}
	return cols, row, nil
}

// deleteTuple 主键元组：DELETE 取 Pk；主键变更的旧键取前像（BEFORE 中主键列
// 永不脱敏/排除，transform 保证存在）。
func deleteTuple(ev *model.Event, src map[string]any) ([]string, []any, error) {
	if len(ev.Pk) == 0 {
		return nil, nil, sink.Permanent(fmt.Errorf("mysql sink: %s.%s %s 事件无主键，无法寻址",
			ev.Database, ev.Table, ev.Type))
	}
	pkCols := sortedKeys(ev.Pk)
	old := make([]any, 0, len(pkCols))
	for _, c := range pkCols {
		v, ok := src[c]
		if !ok {
			return nil, nil, sink.Permanent(fmt.Errorf("mysql sink: %s.%s 主键列 %s 缺失于像",
				ev.Database, ev.Table, c))
		}
		old = append(old, v)
	}
	return pkCols, old, nil
}

// pkChanged 判断 UPDATE 是否改了主键（Before 像 vs 后像 Pk）。
func pkChanged(ev *model.Event) bool {
	for k, after := range ev.Pk {
		before, ok := ev.Before[k]
		if !ok || !valueEqual(before, after) {
			return true
		}
	}
	return false
}

func valueEqual(a, b any) bool {
	switch x := a.(type) {
	case nil:
		return b == nil
	case []byte:
		y, ok := b.([]byte)
		return ok && string(x) == string(y)
	case string:
		y, ok := b.(string)
		return ok && x == y
	default:
		return a == b
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func flatten(rows [][]any) []any {
	out := make([]any, 0, len(rows)*4)
	for _, r := range rows {
		out = append(out, r...)
	}
	return out
}
