package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// expectedMetrics 11 §1 全清单 + 多源新增（存在性测试基准，PRD 07 §1）。
var expectedMetrics = []string{
	"gocdc_events_in_total",
	"gocdc_events_out_total",
	"gocdc_events_failed_total",
	"gocdc_bytes_total",
	"gocdc_convert_errors_total",
	"gocdc_message_oversize_total",
	"gocdc_event_lag_seconds",
	"gocdc_position_lag_seconds",
	"gocdc_master_position_lag_bytes",
	"gocdc_sink_batch_duration_seconds",
	"gocdc_sink_retry_total",
	"gocdc_worker_queue_depth",
	"gocdc_watermark_seq",
	"gocdc_last_saved_seq",
	"gocdc_pipeline_paused",
	"gocdc_position_save_failures_total",
	"gocdc_stock_progress_ratio",
	"gocdc_stock_rows_total",
	"gocdc_leader_state",
	"gocdc_leader_term",
	"gocdc_sources",
	"gocdc_source_up",
}

// initAll 给每个 Vec 指标触发一个子项——未使用的 Vec 不产生 MetricFamily（prometheus 惯例）。
// 源私有指标统一传 sid（source label 第一维）。
func initAll(m *Metrics) {
	const sid = "default"
	m.EventsIn.WithLabelValues(sid, "d", "t", "insert").Inc()
	m.EventsOut.WithLabelValues(sid, "d", "t", "insert", "mysql").Inc()
	m.EventsFailed.WithLabelValues(sid, "mysql", "timeout").Inc()
	m.BytesOut.WithLabelValues(sid, "mysql").Add(1)
	m.ConvertErrors.WithLabelValues(sid, "d", "t", "c", "fail_event").Inc()
	m.MessageOversize.WithLabelValues(sid, "d", "t", "kafka").Inc()
	m.EventLagSeconds.WithLabelValues(sid).Set(0)
	m.PositionLagSeconds.WithLabelValues(sid).Set(0)
	m.SetMasterPositionLagBytes(sid, 0)
	m.SinkBatchDuration.WithLabelValues(sid, "mysql", "d", "t").Observe(0.01)
	m.SinkRetryTotal.WithLabelValues(sid, "mysql").Inc()
	m.WorkerQueueDepth.WithLabelValues(sid, "0").Set(0)
	m.WatermarkSeq.WithLabelValues(sid).Set(0)
	m.LastSavedSeq.WithLabelValues(sid).Set(0)
	m.PipelinePaused.WithLabelValues(sid, "d", "t", "ddl").Set(0)
	m.PositionSaveFailures.WithLabelValues(sid).Inc()
	m.StockProgressRatio.WithLabelValues(sid, "d", "t").Set(0)
	m.StockRowsTotal.WithLabelValues(sid, "d", "t").Inc()
	m.Sources.WithLabelValues("ready").Set(1)
	m.SourceUp.WithLabelValues(sid).Set(1)
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

// TestMetricLabels 关键指标的标签集与 11 §1 + 多源 source label（PRD 07 §1）一致。
func TestMetricLabels(t *testing.T) {
	m := New()
	const sid = "src1"
	m.EventsIn.WithLabelValues(sid, "db1", "t1", "insert").Inc()
	m.EventsOut.WithLabelValues(sid, "db1", "t1", "insert", "mysql").Inc()
	m.SinkBatchDuration.WithLabelValues(sid, "mysql", "db1", "t1").Observe(0.01)
	m.WorkerQueueDepth.WithLabelValues(sid, "0").Set(3)
	m.PipelinePaused.WithLabelValues(sid, "db1", "t1", "ddl").Set(1)
	m.EventLagSeconds.WithLabelValues(sid).Set(1)
	m.LeaderState.Set(1)

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
	// 源私有指标必含 source；cluster 级指标必不含
	for name, wantParts := range map[string][]string{
		"gocdc_events_in_total":             {"source", "db", "table", "type"},
		"gocdc_events_out_total":            {"source", "sink"},
		"gocdc_pipeline_paused":             {"source", "reason"},
		"gocdc_event_lag_seconds":           {"source"},
		"gocdc_sink_batch_duration_seconds": {"source"},
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
	if mf, ok := byName["gocdc_leader_state"]; !ok {
		t.Fatal("指标 gocdc_leader_state 不存在")
	} else {
		for _, l := range mf.labels {
			if l == "source" {
				t.Error("cluster 级指标 gocdc_leader_state 不应带 source 标签")
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

// TestMasterLagPerSource master lag 采样按源隔离读取（多源 /status 口径）。
func TestMasterLagPerSource(t *testing.T) {
	m := New()
	m.SetMasterPositionLagBytes("src-a", 1024)
	if v, ok := m.MasterPositionLagBytesValue("src-a"); !ok || v != 1024 {
		t.Fatalf("src-a 采样读取: %v %v", v, ok)
	}
	if _, ok := m.MasterPositionLagBytesValue("src-b"); ok {
		t.Fatal("src-b 未采样应返回 false")
	}
}

// gatherPaused 汇总当前 gocdc_pipeline_paused 各 series 的值
//（"source|db|table|reason" → 值；label 取值按名定位，不依赖 Gather 排序）。
func gatherPaused(t *testing.T, m *Metrics) map[string]float64 {
	t.Helper()
	families, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, mf := range families {
		if mf.GetName() != "gocdc_pipeline_paused" {
			continue
		}
		for _, pm := range mf.Metric {
			lv := map[string]string{}
			for _, l := range pm.Label {
				lv[l.GetName()] = l.GetValue()
			}
			key := strings.Join([]string{lv["source"], lv["db"], lv["table"], lv["reason"]}, "|")
			out[key] = pm.GetGauge().GetValue()
		}
	}
	return out
}

// TestClearPipelinePaused 按源清零：管线重启清除上一任期全部 paused 标记，
// 其余源不受影响——不清零会让 GocdcPipelinePaused（critical）在恢复后永久误报。
func TestClearPipelinePaused(t *testing.T) {
	m := New()
	m.PipelinePaused.WithLabelValues("src-a", "db1", "t1", "ddl").Set(1)
	m.PipelinePaused.WithLabelValues("src-a", "db2", "t2", "fatal").Set(1)
	m.PipelinePaused.WithLabelValues("src-b", "db1", "t1", "sink").Set(1)

	m.ClearPipelinePaused("src-a")

	got := gatherPaused(t, m)
	if len(got) != 1 {
		t.Fatalf("src-a 清零后应只剩 src-b 一条 series: %v", got)
	}
	if v := got["src-b|db1|t1|sink"]; v != 1 {
		t.Fatalf("src-b 的 paused 标记不应被波及: %v", got)
	}

	// 再置入 + 再清：幂等可反复调用（管线每轮重启前清理）
	m.PipelinePaused.WithLabelValues("src-a", "db1", "t1", "convert").Set(1)
	m.ClearPipelinePaused("src-a")
	if got = gatherPaused(t, m); len(got) != 1 {
		t.Fatalf("重复清理后应仍只剩 src-b: %v", got)
	}
}
