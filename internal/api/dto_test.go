package api

import (
	"testing"
	"time"

	"gocdc/internal/pipeline"
)

// TestPipelineSnapshot_ApplyDDLPause DDL 决策等待只在 running 基态上叠加
// paused；其余基态（stock/stopped/idle/致命 paused）一律不被覆盖。
func TestPipelineSnapshot_ApplyDDLPause(t *testing.T) {
	dp := &pipeline.DDLPauseState{SQL: "SAVEPOINT `sp1`", Since: time.Unix(1700000000, 0)}

	// running 基态 → paused + 指引文案 + Since 换源
	s := PipelineSnapshot{State: PipeRunning, Error: "", Since: time.Unix(1699999000, 0)}
	s.ApplyDDLPause(dp)
	if s.State != PipePaused {
		t.Fatalf("running 应叠加为 paused: %s", s.State)
	}
	if s.Error == "" || s.Since != dp.Since {
		t.Fatalf("Error/Since 未覆盖: %+v", s)
	}

	// 非 running 基态（stock/stopped/idle/fatal paused）不覆盖
	for _, base := range []string{PipeStock, PipeStopped, PipeIdle, PipePaused} {
		s := PipelineSnapshot{State: base, Error: "base"}
		s.ApplyDDLPause(dp)
		if s.State != base || s.Error != "base" {
			t.Fatalf("基态 %s 不应被覆盖: %+v", base, s)
		}
	}

	// nil 等待（无 DDL 挂起）不动
	s = PipelineSnapshot{State: PipeRunning}
	s.ApplyDDLPause(nil)
	if s.State != PipeRunning {
		t.Fatalf("nil 应不动作: %+v", s)
	}
}
