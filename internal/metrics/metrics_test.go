package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// expectedMetrics 11 §1 全清单（存在性测试基准）。
var expectedMetrics = []string{
	"gocdc_events_in_total",
	"gocdc_events_out_total",
	"gocdc_events_failed_total",
	"gocdc_bytes_total",
	"gocdc_convert_errors_total",
	"gocdc_deadletter_total",
	"gocdc_message_oversize_total",
	"gocdc_event_lag_seconds",
	"gocdc_position_lag_seconds",
	"gocdc_master_position_lag_bytes",
	"gocdc_sink_batch_duration_seconds",
	"gocdc_sink_retry_total",
	"gocdc_worker_queue_depth",
	"gocdc_worker_queue_oldest_seconds",
	"gocdc_watermark_seq",
	"gocdc_last_saved_seq",
	"gocdc_pipeline_paused",
	"gocdc_position_save_failures_total",
	"gocdc_stock_progress_ratio",
	"gocdc_stock_rows_total",
	"gocdc_leader_state",
	"gocdc_leader_term",
	"gocdc_etcd_request_duration_seconds",
}

// initAll 给每个 Vec 指标触发一个子项——未使用的 Vec 不产生 MetricFamily（prometheus 惯例）。
func initAll(m *Metrics) {
	m.EventsIn.WithLabelValues("d", "t", "insert").Inc()
	m.EventsOut.WithLabelValues("d", "t", "insert", "mysql").Inc()
	m.EventsFailed.WithLabelValues("mysql", "timeout").Inc()
	m.BytesOut.WithLabelValues("mysql").Add(1)
	m.ConvertErrors.WithLabelValues("d", "t", "c", "fail_event").Inc()
	m.DeadLetter.WithLabelValues("d", "t", "kafka", "oversize").Inc()
	m.MessageOversize.WithLabelValues("d", "t", "kafka").Inc()
	m.SinkBatchDuration.WithLabelValues("mysql", "d", "t").Observe(0.01)
	m.SinkRetryTotal.WithLabelValues("mysql").Inc()
	m.WorkerQueueDepth.WithLabelValues("0").Set(0)
	m.PipelinePaused.WithLabelValues("d", "t", "ddl").Set(0)
	m.StockProgressRatio.WithLabelValues("d", "t").Set(0)
	m.StockRowsTotal.WithLabelValues("d", "t").Inc()
	m.EtcdRequestSeconds.WithLabelValues("put").Observe(0.001)
}

// TestAllMetricsRegistered 全部指标已注册（验收口径：curl /metrics 见 gocdc_ 前缀）。
func TestAllMetricsRegistered(t *testing.T) {
	m := New()
	initAll(m)
	families, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, mf := range families {
		got[mf.GetName()] = true
	}
	for _, name := range expectedMetrics {
		if !got[name] {
			t.Errorf("指标 %s 未注册", name)
		}
	}
}

// TestMetricLabels 关键指标的标签集与 11 §1 定义一致。
func TestMetricLabels(t *testing.T) {
	m := New()
	m.EventsIn.WithLabelValues("db1", "t1", "insert").Inc()
	m.EventsOut.WithLabelValues("db1", "t1", "insert", "mysql").Inc()
	m.SinkBatchDuration.WithLabelValues("mysql", "db1", "t1").Observe(0.01)
	m.WorkerQueueDepth.WithLabelValues("0").Set(3)
	m.PipelinePaused.WithLabelValues("db1", "t1", "ddl").Set(1)
	m.EtcdRequestSeconds.WithLabelValues("put").Observe(0.002)

	families, _ := m.registry.Gather()
	byName := map[string]struct {
		labels []string
	}{}
	for _, mf := range families {
		var labels []string
		for _, l := range mf.Metric[0].Label {
			labels = append(labels, l.GetName())
		}
		byName[mf.GetName()] = struct{ labels []string }{labels}
	}
	for name, wantParts := range map[string][]string{
		"gocdc_events_in_total":  {"db", "table", "type"},
		"gocdc_events_out_total": {"sink"},
		"gocdc_pipeline_paused":  {"reason"},
	} {
		mf, ok := byName[name]
		if !ok {
			t.Fatalf("指标 %s 不存在", name)
		}
		got := map[string]bool{}
		for _, l := range mf.labels {
			got[l] = true
		}
		for _, w := range wantParts {
			if !got[w] {
				t.Errorf("指标 %s 缺少标签 %q", name, w)
			}
		}
	}
}

// TestHandlers /metrics 输出、/healthz 恒 200、/readyz 探针语义。
func TestHandlers(t *testing.T) {
	m := New()
	initAll(m)
	s := NewServer()
	ts := httptest.NewServer(s.Handler(m))
	defer ts.Close()

	// /metrics：包含 gocdc_ 前缀指标
	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("/metrics: %v %v", resp, err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(string(body), "gocdc_events_in_total") {
		t.Fatal("/metrics 应包含 gocdc_events_in_total")
	}

	// /healthz：恒 200（liveness 不查外部依赖）
	resp, _ = http.Get(ts.URL + "/healthz")
	if resp.StatusCode != 200 {
		t.Fatalf("/healthz 应 200，实际 %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// /readyz：探针失败 → 503 且带探针名
	s.RegisterProbe("config_loaded", func() bool { return false })
	resp, _ = http.Get(ts.URL + "/readyz")
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 503 || !strings.Contains(string(b), "config_loaded") {
		t.Fatalf("/readyz 探针失败应 503 带名字，实际 %d %s", resp.StatusCode, b)
	}

	// 探针通过 → 200
	s.RegisterProbe("config_loaded", func() bool { return true })
	resp, _ = http.Get(ts.URL + "/readyz")
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("/readyz 探针通过应 200，实际 %d", resp.StatusCode)
	}
}
