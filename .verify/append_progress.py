import io

entry = """
- 2026-10-30 **使用指导 + 测试用例全集 + 反方对抗验证 + 集成测试补强（commit a80541a）**：① docs/GUIDE.md 新手完整上手指南（源库/目标库授权 SQL、单点/集群部署、全配置字段、MQ topic 命名与 RMQ FIFO 预建、app.yml→config.json 迁移四步映射、Web 运维速查、生产 checklist、FAQ）；② docs/TESTCASES.md 全场景 case 矩阵 ~70 条（L1 单测/L2 集成/L3 验收/L4 手工四级分层）+「11 个集成测试为何没抓住 8 个 P0」缺口根因分析（路径未覆盖/时序类/组合态缺失/竞态窗口/装配层不可达五类）+ ADV-01~14 反方对抗验证全记录；③ 4 个 P0 根因集成测试（testcontainers 真跑全绿）：同名默认+3 表并行 stock（双实例）/etcd 停机 OnLost 时延 1.4s≤8s/GTID rotate 集合保持/过期 DDL approve 放行；④ 对抗再抓 3 bug 修复：column_count_mismatch 无视策略强制 fatal（回拨重放撞已放行 ADD COLUMN 即死锁）→ image() 走 on_convert_error 三策略；已删表历史行重放 fatal → 跳过+告警+ack（防水位洞）；auth_disabled=true 凭据校验豁免不可达（validate.go 加 ！AuthDisabled）；⑤ ADV-07 restock 跨会话重扫 1201 行收敛零重复。回归：Windows build/vet/test 15 包 + WSL -race 15 包全绿。**架构教训（TESTCASES §0）**：集成测试必须覆盖全默认参数路径/外部组件故障时序/配置组合矩阵/挂起审批竞态窗口；装配层闭包 bug 只有端到端能抓——关键默认路径必须有一条端到端。
"""

with io.open(r"D:\workers\worker\qingfly\gocdc-mysql\docs\PROGRESS.md", encoding="utf-8") as f:
    lines = f.readlines()

lines.insert(114, entry)

with io.open(r"D:\workers\worker\qingfly\gocdc-mysql\docs\PROGRESS.md", "w", encoding="utf-8", newline="\n") as f:
    f.writelines(lines)
print("inserted")
