package main

import (
	"context"
	"strings"
	"testing"
)

// 人工暂停/恢复的前置护栏（13 §3 逐源版）：不碰 etcd 的分支——
// 非 leader 拒绝暂停、状态机 CAS 防重。etcd 读写路径由集成验证覆盖。
//
// 护栏语义（55 组 PP-05 首轮实证的教训，多源版沿袭）：resume 只认 opPaused
// 不认 leading——暂停即源流程退出，「暂停中的 Leader 源」与「热备/失位」在
// 源流程槽上不可区分；失位场景由 onLost 清 opPaused 移交责任。

// testSourceState 最小可构造源态（暂停护栏用不到 etcd/装配产物）。
func testSourceState() *sourceState {
	return &sourceState{sid: "test-src"}
}

func TestPausePipeline_NotLeading(t *testing.T) {
	a := &app{} // zero value：源流程槽空（热备/未当选/流程已退出/源未就绪）
	src := testSourceState()
	err := a.pauseSourcePipeline(context.Background(), src)
	if err == nil || !strings.Contains(err.Error(), "不承担该源管线") {
		t.Fatalf("非 leader 应拒绝暂停: %v", err)
	}
	if src.opPaused.Load() {
		t.Fatal("拒绝路径不得置位 opPaused")
	}
}

func TestPausePipeline_AlreadyPaused(t *testing.T) {
	a := &app{}
	src := testSourceState()
	src.opPaused.Store(true) // 暂停态（流程已退出，槽可空）
	err := a.pauseSourcePipeline(context.Background(), src)
	if err == nil || !strings.Contains(err.Error(), "已处于人工暂停状态") {
		t.Fatalf("重复暂停应被拒: %v", err)
	}
}

func TestResumePipeline_NotPaused(t *testing.T) {
	a := &app{}
	src := testSourceState() // opPaused=false（运行中/致命暂停/热备均如此）
	err := a.resumeSourcePipeline(context.Background(), src)
	if err == nil || !strings.Contains(err.Error(), "未处于人工暂停状态") {
		t.Fatalf("非暂停态应拒绝恢复: %v", err)
	}
	if src.opPaused.Load() {
		t.Fatal("拒绝路径不得置位 opPaused")
	}
}

// onLost 清 opPaused 的失位移交语义依赖 met/reg 真实实例（onLost 内联调
// LeaderState/Registrar），L1 不可构造——由 L3 组55 PP-08 重启场景覆盖。
