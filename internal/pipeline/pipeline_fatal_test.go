package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"gocdc/internal/model"
	"gocdc/internal/source"
)

// 拉取侧 Source.Run 返回不可恢复错误（起始位点非法——use_gtid=true 而位点
// gtid_set 为空）时，管线必须熔断退出而非静默挂死：若只 Warn，dispatcher
// 会阻塞在永远无数据的 srcCh 上、队列不关、水位冻结——进程存活、Web 正常、
// 同步静默死亡。
func TestPipeline_SourceInvalidPositionFatal(t *testing.T) {
	src := source.New(source.Config{UseGTID: true}, nil) // validateStart 将拒绝空 gtid_set
	p := New(Options{
		Source: src,
		Store:  &memStore{gateOK: true},
	})
	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background(), model.Position{Name: "binlog.000001", Pos: 4}, nil) }()
	select {
	case err := <-done:
		if !errors.Is(err, source.ErrInvalidPosition) {
			t.Fatalf("应以拉取侧首因熔断退出: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("拉取侧不可恢复错误应熔断管线，实际挂死（静默死亡回归）")
	}
}
