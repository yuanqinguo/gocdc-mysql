# gocdc-mysql 审查条目全量复审（2026-11-16）

> 本文件是 [REVIEW-2026-11-16.md](REVIEW-2026-11-16.md) 全部审查条目的**逐条复核**：
> 每一条都回当前代码取证（`read`/`grep` + 行号），给出「已修复 / 文档化 / 误报撤销 /
> 仍有效未修」四类结论。不含任何推测——每条结论都有可复核的证据位置。

## 1. 复审方法

1. 基线：全量 `go test ./... -count=1`、`go vet ./...`。
2. 逐条按原报告的行号定位到当前代码，确认修复产物是否存在且语义正确；
   未修复项确认缺陷在当前代码中仍然成立。
3. 中文/编码类条目一律用 `read` 工具取证（**不用** pwsh `Get-Content`——GBK 控制台
   会把 UTF-8 中文显示成乱码，曾导致首轮 P4-1 误报）。
4. 前端条目（三轮）按行号逐条复核，并修正子代理结论中的计数偏差。

## 2. 基线验证

| 检查 | 结果 |
|---|---|
| `go test ./... -count=1` | ✅ 18 包全 ok，0 FAIL |
| `go vet ./...` | ✅ exit 0 |
| 前端 `typecheck/lint/test(43)/build` | ✅（前轮已验证，本轮未重复跑） |

## 3. 结论总表（27 条）

| ID | 级别 | 结论 | 证据（当前代码） |
|---|---|---|---|
| P0-1 config 测试红灯 | 高 | ✅ 已修复 | `config_test.go:296` 期望含两条报错 |
| P1-1 enum/set 小写化 | 高 | ✅ 已修复 | `normalize.go:41-44,68,84`；`schema_test.go:36` |
| P1-2 unsigned zerofill | 高 | ✅ 已修复 | `normalize.go:84`；`schema_test.go:39-41` |
| P2-1 解锁失败带锁扫描 | 高 | ✅ 已修复 | `session.go:160,174,178` |
| P2-2 P 读取晚于快照 | 中高 | ✅ 已修复 | `session.go:128`（P）< `135`（快照事务） |
| P2-3 redials 累计误杀 | 中 | ✅ 已修复 | `scan.go:98`（ctx 感知）、`109`（成功归零） |
| P2-4 续扫虚设/取 max | 中高 | ✅ 已修复 | `service.go:95`（续扫）、`138`（取小）、`243`（failed 保留断点） |
| P3-1 schema 缓存无失效 | 中 | ✅ 已修复 | `schema.go:78`（Drop）；`worker.go:426,498`（接线） |
| P3-2 凭据明文 Cookie | 中 | ✅ 过渡方案已修 | `session.ts:17,37,42`（sessionStorage，无 document.cookie） |
| P3-3 无登录限速 | 低中 | ✅ 已修复 | `auth.go:35,42,76,81`；`auth_throttle_test.go` 存在 |
| P4-1 中文乱码 | 低 | ❌ 误报撤销 | `Makefile:1-3`、`.gitignore:1` 中文正常（`read` 工具口径）；`embed.go:71` 正常 |
| P4-2 Throttler 死代码 | 低 | ✅ 已修复 | `throttle.go` 已删除；全仓 `Throttler` 引用 0 |
| P4-3 dist 半跟踪 | 低 | ✅ 文档化 | `web/README.md:25`（重建说明）；Makefile `build: web-build` 前置 |
| P4-4 演示弱口令 | 低 | ✅ 文档化 | `deploy/README.md:3,9,58`（安全告示 + TLS 要求） |
| P4-5 web/go.mod | 低 | ❌ 误报撤销 | `web/go.mod` 已恢复（隔离 node_modules Go fixture，实测必要） |
| P4-5 golangci 入 CI | 低 | ⚠️ 仍开放 | CI 未接入（`.cnb.yml` 无 lint job） |
| 二轮-1 datetime 字面量默认 | 中 | ✅ 已修复 | `ddlgen.go:511,575`（`isTimestampFuncDefault`） |
| 二轮-2a ON UPDATE 丢弃 | 低 | ⚠️ **仍有效未修** | `ddlgen.go:431` 仅提取 `auto_increment` |
| 二轮-2b Redactor/LogRow 死代码 | 低 | ⚠️ **仍有效未修** | `NewRedactor`/`LogRow` 生产调用点 0（仅定义+测试） |
| 三轮-1 Web 端口不 fail-fast | 中 | ⚠️ **仍有效未修** | `admin.go:238` 签名无 stop；`251` 仅记日志 |
| 三轮-2 对账不感知 exclude/mask | 中 | ⚠️ **仍有效未修** | `verify.go` 全文无 exclude/mask 概念 |
| 三轮-3 mask 参数往返丢失 | 中 | ⚠️ **仍有效未修** | `ruleDraft.ts:127` 仅 `${col}=${spec.type}` |
| 三轮-4 轮询失败整页白屏 | 中 | ⚠️ **仍有效未修** | `DashboardPage.tsx:152` `statusError ?` 优先级最高 |
| 三轮-5 顶栏 Chip 回退错误 | 中 | ⚠️ **仍有效未修** | `AppLayout.tsx:77` `?? 'idle'` |
| 三轮-6 mutateAsync 裸调用 | 中 | ⚠️ **仍有效未修（修正：5 处）** | `131,283,346,429,438` |
| 三轮-7 让位入口判定不一致 | 低 | ⚠️ **仍有效未修** | 单源 `292`（leader）vs 聚合 `396/414`（leader\|\|single） |
| 三轮-8 暂停框假 loading | 低 | ⚠️ **仍有效未修（限聚合视图）** | `440` 无 sid 守卫（对照 `426` 正确） |
| 三轮-9 currentSid 死胡同 | 低 | ⚠️ **仍有效未修** | `DashboardPage.tsx:156` 重试仅 refetchStatus |

**统计**：已修复 12 · 文档化 2 · 误报撤销 2 · 仍有效未修 11（其中 1 项部分开放）。

## 4. 复审对原结论的修正（重要）

1. **三轮-6 数量修正**：原报告因子代理给的是 5 处、我首轮 grep 只匹配无参形态得到 3 处。
   全量复核确认为 **5 处**：`131/283/346`（无参）+ `429/438`（带参）均为裸 `mutateAsync`。
2. **三轮-8 范围澄清**：`DashboardPage.tsx:348` 也有同款 `loading: pausePipeline.isPending`，
   但那是**单源视图**（该页只有一个源，不存在跨卡片串扰），不属于缺陷；
   缺陷仅限聚合视图 `440`（多卡片共用同一 mutation）。
3. **P4-5 拆分**：原条目合并了两件事——`web/go.mod`（误报，已撤销）与
   golangci-lint 入 CI（真实遗留，仍开放）。
4. **P3-2 定性**：确认是**过渡方案**已落地（sessionStorage），报告主方案
   （服务端 HttpOnly 会话）仍未实施，不应当作"已闭环"。

## 5. 仍开放项的处置建议（按风险排序）

| 顺序 | 条目 | 理由 | 修复成本 |
|---|---|---|---|
| 1 | 三轮-3 mask 去盐 | **安全回归**：hash 脱敏静默去盐，输出值全变且无告警 | 小（编辑器阻断带参对象即可先行） |
| 2 | 三轮-1 Web 端口不 fail-fast | headless 会让集群重启编排永久卡在 peer healthz | 小（传 stop、对齐 metrics 口径） |
| 3 | 三轮-2 对账假差异 | 敏感表恰是最需要对账的表，工具结论误导 | 中（注入规则 + 口径设计） |
| 4 | 三轮-4 轮询白屏 | 值班监控周期性闪烁、操作按钮消失 | 小（改条件 + 顶部提示条） |
| 5 | 三轮-6 未处理 rejection | 污染全局错误监控，掩盖真实异常 | 极小（`mutate()` 或 `.catch`） |
| 6 | 三轮-5 顶栏 Chip | 把"源不存在/未知"显示为"空闲"，误导运维 | 小（映射补齐 + 统一口径） |
| 7 | 二轮-2a ON UPDATE | CDC 路径无数据影响（目标值由 binlog 直写），仅直写目标时分叉 | 小 |
| 8 | 二轮-2b Redactor 死代码 | 当前生产不打印行数据，无暴露；接入前须先修 key 大小写 | 小（删除或修 key） |
| 9 | 三轮-7/8/9 | UI 一致性与死胡同 | 小 |
| 10 | P4-5 golangci CI | 需选定镜像/版本策略 | 中（CI 侧） |

## 6. 复审结论

- 首轮与二轮修复**全部真实落地且未回退**：11 + 1 项均有代码证据，测试基线全绿。
- 两条误报（P4-1 编码、P4-5 web/go.mod）**维持撤销**，本轮用 `read` 工具再次取证。
- 11 条未修项**全部仍然成立**，且三轮-3（脱敏去盐）是其中唯一的安全相关项，
  建议优先处理。
- 未发现"修复引入新缺陷"或"修复点失效"的情况。
