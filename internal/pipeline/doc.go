// Package pipeline 实现并行管线：Dispatcher（seq 分配 + 表级哈希分片 + 有界队列）、
// Worker（转换/脱敏/发送）、Watermark 水位线、DDL/Rotate Barrier 与优雅停机 drain。
//
// 不变式：位点只能由 Watermark.base 推进（04 §5），全链路队列必须有界（09 §1）。
// 权威设计：docs/arch/04-pipeline-parallel.md。
package pipeline
