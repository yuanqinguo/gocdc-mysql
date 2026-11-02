// metrics.go：指标定义与注册（各指标语义见字段注释，清单见 11 §1 +
// 多源 PRD 07 §1 label 化）。
//
// 多源 label 约定：源私有指标统一带 source label（第一维），调用点传 sid；
// cluster 级指标（leader/etcd）不带。单源 v1 部署升级后指标多出
// source="default"——不带 source 的 PromQL 选择器仍然命中（可预期破坏性，
// 升级说明标注）。

package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics 全部业务指标的集合（11 §1.1~1.4 + 多源新增）。
type Metrics struct {
	registry *prometheus.Registry

	// ── 1.1 吞吐与处理量（source, …）──
	EventsIn        *prometheus.CounterVec // source, db, table, type(insert/update/delete/ddl)
	EventsOut       *prometheus.CounterVec // source, db, table, type, sink（snapshot 另计 phase 标签）
	EventsFailed    *prometheus.CounterVec // source, sink, reason
	BytesOut        *prometheus.CounterVec // source, sink
	ConvertErrors   *prometheus.CounterVec // source, db, table, column, action(04 §7 on_convert_error)
	DeadLetter      *prometheus.CounterVec // source, db, table, sink, reason（07 §6，恒应≈0）
	MessageOversize *prometheus.CounterVec // source, db, table, sink（05 §7 预检）

	// ── 1.2 延迟（source 维度）──
	EventLagSeconds        *prometheus.GaugeVec // source：端到端 = now − watermark 事件 binlog 时间戳
	PositionLagSeconds     *prometheus.GaugeVec // source：位点滞后（事务时间视角）
	MasterPositionLagBytes *prometheus.GaugeVec // source：与 SHOW MASTER STATUS 差距
	SinkBatchDuration      *prometheus.HistogramVec // source, sink, db, table
	SinkRetryTotal         *prometheus.CounterVec   // source, sink

	// masterLagSamples 最近一次 MasterPositionLagBytes 采样值（字节）按源——
	// admin /status 的 caught_up 口径读取（gauge 自身不便反读）。
	masterLagMu      sync.Mutex
	masterLagSamples map[string]float64

	// ── 1.3 管线与资源（source, …）──
	WorkerQueueDepth        *prometheus.GaugeVec // source, worker（背压前兆）
	WorkerQueueOldestSecond *prometheus.GaugeVec // source, worker：队列最老事件滞留时长
	WatermarkSeq            *prometheus.GaugeVec // source：连续水位
	LastSavedSeq            *prometheus.GaugeVec // source：已落盘位点 seq（差值=未落盘窗口）
	PipelinePaused          *prometheus.GaugeVec // source, db, table, reason（ddl/pause/sink 熔断）
	PositionSaveFailures    *prometheus.CounterVec // source：etcd 位点写失败（03 §5.2 告警源）

	// ── 1.4 全量 / HA / etcd ──
	StockProgressRatio *prometheus.GaugeVec   // source, db, table，0~1
	StockRowsTotal     *prometheus.CounterVec // source, db, table
	LeaderState        prometheus.Gauge       // 1=leader（cluster 级，不带 source）
	LeaderTerm         prometheus.Gauge       // election revision（cluster 级）
	EtcdRequestSeconds *prometheus.HistogramVec // op（cluster 级）

	// ── 多源新增（PRD 07 §1）──
	Sources  *prometheus.GaugeVec // state：按 ready/failed/stopped 计数的源数
	SourceUp *prometheus.GaugeVec // source：1=该源就绪且非 failed
}

// New 构建并注册全部指标。
func New() *Metrics {
	m := &Metrics{registry: prometheus.NewRegistry(), masterLagSamples: map[string]float64{}}

	m.EventsIn = newCounter(m.registry, "gocdc_events_in_total",
		"binlog 侧进入事件数", "source", "db", "table", "type")
	m.EventsOut = newCounter(m.registry, "gocdc_events_out_total",
		"各 sink 成功投递数", "source", "db", "table", "type", "sink")
	m.EventsFailed = newCounter(m.registry, "gocdc_events_failed_total",
		"sink 投递失败数（含重试耗尽）", "source", "sink", "reason")
	m.BytesOut = newCounter(m.registry, "gocdc_bytes_total",
		"下发字节量", "source", "sink")
	m.ConvertErrors = newCounter(m.registry, "gocdc_convert_errors_total",
		"值转换异常数（04 §7 on_convert_error）", "source", "db", "table", "column", "action")
	m.DeadLetter = newCounter(m.registry, "gocdc_deadletter_total",
		"进入 DLQ 的事件数（恒应≈0，07 §6）", "source", "db", "table", "sink", "reason")
	m.MessageOversize = newCounter(m.registry, "gocdc_message_oversize_total",
		"消息超限事件数（05 §7 发送前预检）", "source", "db", "table", "sink")

	m.EventLagSeconds = newGauge(m.registry, "gocdc_event_lag_seconds",
		"端到端延迟 = now − watermark 处事件的 binlog 时间戳", "source")
	m.PositionLagSeconds = newGauge(m.registry, "gocdc_position_lag_seconds",
		"位点滞后 = now − 已保存位点对应事务时间", "source")
	m.MasterPositionLagBytes = newGauge(m.registry, "gocdc_master_position_lag_bytes",
		"与源库 SHOW MASTER STATUS 的字节差距", "source")
	m.SinkBatchDuration = newHistogram(m.registry, "gocdc_sink_batch_duration_seconds",
		"sink 批次耗时", prometheus.ExponentialBuckets(0.001, 2, 15), "source", "sink", "db", "table")
	m.SinkRetryTotal = newCounter(m.registry, "gocdc_sink_retry_total",
		"sink 重试次数", "source", "sink")

	m.WorkerQueueDepth = newGauge(m.registry, "gocdc_worker_queue_depth",
		"分片队列深度（背压前兆）", "source", "worker")
	m.WorkerQueueOldestSecond = newGauge(m.registry, "gocdc_worker_queue_oldest_seconds",
		"队列最老事件滞留时长（秒）", "source", "worker")
	m.WatermarkSeq = newGauge(m.registry, "gocdc_watermark_seq",
		"连续水位 seq", "source")
	m.LastSavedSeq = newGauge(m.registry, "gocdc_last_saved_seq",
		"已落盘位点 seq（与 watermark 差值=未落盘窗口）", "source")
	m.PipelinePaused = newGauge(m.registry, "gocdc_pipeline_paused",
		"暂停状态（1=paused；ddl/pause 策略/sink 熔断）", "source", "db", "table", "reason")
	m.PositionSaveFailures = newCounter(m.registry, "gocdc_position_save_failures_total",
		"etcd 位点写失败次数（03 §5.2）", "source")

	m.StockProgressRatio = newGauge(m.registry, "gocdc_stock_progress_ratio",
		"全量进度 0~1", "source", "db", "table")
	m.StockRowsTotal = newCounter(m.registry, "gocdc_stock_rows_total",
		"全量已扫描行数", "source", "db", "table")
	m.LeaderState = newGaugeSingle(m.registry, "gocdc_leader_state",
		"leader 状态（1=leader，0=follower/无主）")
	m.LeaderTerm = newGaugeSingle(m.registry, "gocdc_leader_term",
		"当前 leader term（election revision）")
	m.EtcdRequestSeconds = newHistogram(m.registry, "gocdc_etcd_request_duration_seconds",
		"etcd 操作耗时", prometheus.ExponentialBuckets(0.001, 2, 12), "op")

	m.Sources = newGauge(m.registry, "gocdc_sources",
		"按状态计数的源数（ready/failed/stopped）", "state")
	m.SourceUp = newGauge(m.registry, "gocdc_source_up",
		"1=该源就绪且非 failed（多源 PRD 07 §1）", "source")

	return m
}

// Registry 暴露内部 registry（promhttp handler 用）。
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

func newCounter(r *prometheus.Registry, name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	r.MustRegister(c)
	return c
}

func newCounterSingle(r *prometheus.Registry, name, help string) prometheus.Counter {
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
	r.MustRegister(c)
	return c
}

func newGauge(r *prometheus.Registry, name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
	r.MustRegister(g)
	return g
}

func newGaugeSingle(r *prometheus.Registry, name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	r.MustRegister(g)
	return g
}

// SetMasterPositionLagBytes 采样并写该源 master 位点差 gauge（pipeline.sampleLoop
// 周期调用）；采样值另存供 admin /status 的 caught_up 口径读取（gauge 自身
// 不便反读，避免 API 路径直打源库）。
func (m *Metrics) SetMasterPositionLagBytes(source string, v float64) {
	m.masterLagMu.Lock()
	m.masterLagSamples[source] = v
	m.masterLagMu.Unlock()
	m.MasterPositionLagBytes.WithLabelValues(source).Set(v)
}

// MasterPositionLagBytesValue 该源最近一次采样值；未采样过返回 false
//（stock 期/未当选时该源 gauge 无人写）。
func (m *Metrics) MasterPositionLagBytesValue(source string) (float64, bool) {
	m.masterLagMu.Lock()
	defer m.masterLagMu.Unlock()
	v, ok := m.masterLagSamples[source]
	return v, ok
}

func newHistogram(r *prometheus.Registry, name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
	r.MustRegister(h)
	return h
}
