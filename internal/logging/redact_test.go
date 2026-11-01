package logging

import (
	"bytes"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// fakeRedactor 测试桩：排除 exclude 列、把 phone 列替换为固定掩码值，
// 其余原样透传（真实实现在 transform 包）。
type fakeRedactor struct{ exclude map[string]bool }

func (f fakeRedactor) Redact(schema, table string, row map[string]any) map[string]any {
	out := make(map[string]any, len(row))
	for c, v := range row {
		if f.exclude[c] {
			continue
		}
		if c == "phone" {
			out[c] = "***"
			continue
		}
		out[c] = v
	}
	return out
}

// newLoggedJSON 构建双核 logger：observer（结构化断言）+ JSON 编码写 buffer
// （断言最终真实落日志的编码输出）。
func newLoggedJSON() (*zap.Logger, *bytes.Buffer, *observer.ObservedLogs) {
	core, logs := observer.New(zapcore.InfoLevel)
	buf := &bytes.Buffer{}
	jsonCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(buf), zapcore.InfoLevel)
	return zap.New(zapcore.NewTee(core, jsonCore)), buf, logs
}

// assertNoLeak 断言原始敏感值不出现在编码输出（message+全部字段）任何位置。
func assertNoLeak(t *testing.T, out, secret string) {
	t.Helper()
	if strings.Contains(out, secret) {
		t.Fatalf("原始敏感值 %q 泄漏到日志输出:\n%s", secret, out)
	}
}

// TestLogRow_Redacted 验证脱敏视图：掩码值可见、exclude 列不存在、
// 原始敏感值绝不出现在日志（红线）。
func TestLogRow_Redacted(t *testing.T) {
	logger, buf, _ := newLoggedJSON()

	row := map[string]any{"id": 1, "phone": "13800000000", "name": "alice"}
	red := fakeRedactor{exclude: map[string]bool{"name": true}}
	LogRow(logger, zapcore.InfoLevel, red, "edu", "admin_user", row, "row sample")

	out := buf.String()
	for _, want := range []string{`"col.id":1`, `"col.phone":"***"`, `"schema":"edu"`, `"table":"admin_user"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("日志输出应包含 %s:\n%s", want, out)
		}
	}
	if strings.Contains(out, `"col.name"`) {
		t.Fatalf("exclude 列不应出现在日志:\n%s", out)
	}
	assertNoLeak(t, out, "13800000000")
	assertNoLeak(t, out, "alice")
}

// TestLogRow_NilRedactor 验证无 redactor 时只打列名清单，绝不输出任何列值。
func TestLogRow_NilRedactor(t *testing.T) {
	logger, buf, _ := newLoggedJSON()

	row := map[string]any{"phone": "13800000000", "id": 1, "name": "alice"}
	LogRow(logger, zapcore.InfoLevel, nil, "edu", "admin_user", row, "row sample")

	out := buf.String()
	want := `"columns":["id","name","phone"]`
	if !strings.Contains(out, want) {
		t.Fatalf("日志输出应包含 %s（排序后的列名清单）:\n%s", want, out)
	}
	if strings.Contains(out, `"col.`) {
		t.Fatalf("无 redactor 时不得输出列值字段:\n%s", out)
	}
	assertNoLeak(t, out, "13800000000")
	assertNoLeak(t, out, "alice")
}

// TestLogRow_NilRow 验证 nil 行不 panic 且不输出列值。
func TestLogRow_NilRow(t *testing.T) {
	logger, buf, logs := newLoggedJSON()

	LogRow(logger, zapcore.InfoLevel, fakeRedactor{}, "edu", "t", nil, "nil row")
	LogRow(logger, zapcore.InfoLevel, nil, "edu", "t", nil, "nil row, nil redactor")

	if n := logs.Len(); n != 2 {
		t.Fatalf("应记录 2 条，实际 %d 条", n)
	}
	if strings.Contains(buf.String(), `"col.`) {
		t.Fatalf("nil 行不得输出列值字段:\n%s", buf.String())
	}
}
