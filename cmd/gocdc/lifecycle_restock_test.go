package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

// restock 触发重走的前置护栏（restockRestart，不碰 etcd 的分支）：
// 失位说明性报错、运行态排空清槽、排空超时不重入（防与旧流程抢槽）。
// etcd 读写/真实流程重走路径由集成验证覆盖。

func TestRestockRestart_NotLeading(t *testing.T) {
	a := &app{} // zero value：源流程槽空且无任期（热备/失位/致命停源）
	src := testSourceState()
	err := a.restockRestart(context.Background(), src)
	if err == nil || !strings.Contains(err.Error(), "接管节点当选后将自动重扫") {
		t.Fatalf("无任期应返回失位说明（标记已清即为兑现）: %v", err)
	}
}

func TestRestockRestart_RunningDrainsThenReportsLost(t *testing.T) {
	a := &app{}
	src := testSourceState()
	fctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	src.pipeMu.Lock()
	src.pipeCancel = cancel
	src.pipeDone = done
	src.pipeMu.Unlock()
	go func() { // 模拟 runSourceFlow 退出序：cancel → 清槽 → close(done)
		<-fctx.Done()
		src.pipeMu.Lock()
		src.pipeCancel = nil
		src.pipeDone = nil
		src.pipeMu.Unlock()
		close(done)
	}()
	err := a.restockRestart(context.Background(), src)
	if err == nil || !strings.Contains(err.Error(), "接管节点当选后将自动重扫") {
		t.Fatalf("排空完成后无任期应返回失位说明: %v", err)
	}
	src.pipeMu.Lock()
	cleared := src.pipeCancel == nil && src.pipeDone == nil
	src.pipeMu.Unlock()
	if !cleared {
		t.Fatal("排空等待应等到退出序清槽完成，而非提前返回")
	}
}

func TestRestockRestart_DrainTimeoutNoReentry(t *testing.T) {
	old := restockDrainWait
	restockDrainWait = 50 * time.Millisecond
	defer func() { restockDrainWait = old }()
	a := &app{}
	src := testSourceState()
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	src.pipeMu.Lock()
	src.pipeCancel = cancel
	src.pipeDone = make(chan struct{}) // 永不关闭：旧流程仍在排空
	src.pipeMu.Unlock()
	err := a.restockRestart(context.Background(), src)
	if err == nil || !strings.Contains(err.Error(), "排空未完成") {
		t.Fatalf("排空超时应报错并指引重试: %v", err)
	}
	// 超时路径绝不 spawn 新流程（旧流程未退出时重入会被单槽护栏静默丢弃）
	src.pipeMu.Lock()
	stillSet := src.pipeCancel != nil
	src.pipeMu.Unlock()
	if !stillSet {
		t.Fatal("超时路径不得动源流程槽（由旧流程退出序清理）")
	}
}

// 排空窗口内人工暂停赢下竞速（pause 复用同一 cancel + 置 opPaused + 写持久
// 标志）：restock 不得重走（重走会被暂停闸拦下白跑），且不得覆盖暂停意图——
// 上报说明性错误，标记已清、resume 恢复即重扫。2026-11-04 review 抓到。
func TestRestockRestart_PauseDuringDrainWins(t *testing.T) {
	a := &app{}
	src := testSourceState()
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	src.pipeMu.Lock()
	src.pipeCancel = cancel
	src.pipeDone = done
	src.pipeMu.Unlock()
	go func() { // 模拟排空窗口内 pause 赢：置 opPaused → 走完退出序
		src.opPaused.Store(true)
		src.pipeMu.Lock()
		src.pipeCancel = nil
		src.pipeDone = nil
		src.pipeMu.Unlock()
		close(done)
	}()
	err := a.restockRestart(context.Background(), src)
	if err == nil || !strings.Contains(err.Error(), "人工暂停") || !strings.Contains(err.Error(), "标记已清") {
		t.Fatalf("排空窗口内暂停应上报且不得重走: %v", err)
	}
}
