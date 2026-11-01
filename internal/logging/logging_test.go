package logging

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// newObserved 构建带 observer 的 logger 与可动态调整的级别，供断言。
func newObserved(level zapcore.Level) (*zap.Logger, *observer.ObservedLogs, *zap.AtomicLevel) {
	al := zap.NewAtomicLevelAt(level)
	core, logs := observer.New(al)
	return zap.New(core), logs, &al
}

// TestDynamicLevel 验证级别切换：info 级别过滤 debug，调低后可见。
func TestDynamicLevel(t *testing.T) {
	logger, logs, al := newObserved(zapcore.InfoLevel)

	logger.Debug("invisible")
	if n := logs.Len(); n != 0 {
		t.Fatalf("info 级别下 debug 不应记录，实际 %d 条", n)
	}

	al.SetLevel(zapcore.DebugLevel)
	logger.Debug("visible")
	if n := logs.Len(); n != 1 {
		t.Fatalf("调至 debug 后应记录 1 条，实际 %d 条", n)
	}
}

// TestGlobalLevelAndInjection 验证全局级别读写与任意 logger 注入。
func TestGlobalLevelAndInjection(t *testing.T) {
	logger, _, _ := newObserved(zapcore.InfoLevel)

	old := SetRoot(logger)
	defer SetRoot(old)

	if got := L(); got != logger {
		t.Fatal("SetRoot 后 L() 应返回注入的 logger 实例")
	}

	SetLevel(zapcore.WarnLevel)
	if got := Level(); got != zapcore.WarnLevel {
		t.Fatalf("SetLevel 后 Level()=%v，期望 warn", got)
	}
	SetLevel(zapcore.InfoLevel)
	if got := Level(); got != zapcore.InfoLevel {
		t.Fatalf("SetLevel 后 Level()=%v，期望 info", got)
	}

	// 注入 nil 应回退 no-op，不 panic
	old2 := SetRoot(nil)
	if old2 != logger {
		t.Fatal("SetRoot(nil) 前的旧值应为先前注入的 logger")
	}
	SetRoot(old2)
}

// TestSetup_Levels 验证 Setup 的级别解析：合法值生效、空默认 info、非法值报错。
func TestSetup_Levels(t *testing.T) {
	defer SetRoot(zap.NewNop()) // Setup 会改全局状态，测试后复位

	for _, tc := range []struct {
		in   string
		want zapcore.Level
	}{
		{"debug", zapcore.DebugLevel},
		{"INFO", zapcore.InfoLevel},
		{" warn ", zapcore.WarnLevel},
		{"error", zapcore.ErrorLevel},
		{"", zapcore.InfoLevel}, // 空缺省 info
	} {
		if _, err := Setup(Config{Level: tc.in}); err != nil {
			t.Fatalf("Setup(level=%q) 意外出错: %v", tc.in, err)
		}
		if got := Level(); got != tc.want {
			t.Fatalf("Setup(level=%q) 后全局级别=%v，期望 %v", tc.in, got, tc.want)
		}
	}

	if _, err := Setup(Config{Level: "verbose"}); err == nil {
		t.Fatal("非法级别应报错")
	}
}

// TestEventField 验证生命周期事件字段的键与值（11 §3 固定 event 字段）。
func TestEventField(t *testing.T) {
	logger, logs, _ := newObserved(zapcore.InfoLevel)

	logger.Info("pipeline started", Event(EventPipelineStart), zap.String("reason", "elected"))

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("应记录 1 条，实际 %d 条", len(entries))
	}
	f := entries[0].ContextMap()["event"]
	if s, ok := f.(string); !ok || s != EventPipelineStart {
		t.Fatalf("event 字段=%v，期望 %q", f, EventPipelineStart)
	}
}

// TestParseLevel 表驱动覆盖级别解析。
func TestParseLevel(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    zapcore.Level
		wantErr bool
	}{
		{"debug", zapcore.DebugLevel, false},
		{"info", zapcore.InfoLevel, false},
		{"warn", zapcore.WarnLevel, false},
		{"error", zapcore.ErrorLevel, false},
		{"ERROR", zapcore.ErrorLevel, false},
		{"", zapcore.InfoLevel, false},
		{"fatal", zapcore.FatalLevel, false},
		{"bogus", 0, true},
	} {
		got, err := ParseLevel(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("ParseLevel(%q) 应报错", tc.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("ParseLevel(%q) 意外出错: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("ParseLevel(%q)=%v，期望 %v", tc.in, got, tc.want)
		}
	}
}
