# gocdc 测试用例全集（TESTCASES）

> 目的：全场景可复用验收资产。任何改动后按本清单跑对应层级的自动化验收。
> 分层定义：
> - **L1 单测**：`go test ./...`（无容器，秒级，CI 必跑）
> - **L2 集成**：`go test -tags=integration ./...`（testcontainers 容器，分钟级，CI/发布前必跑）
> - **L3 验收脚本**：真实环境半自动（WSL 容器 + 原生二进制 + `.verify/v2/` 脚本，改版/发布前跑）
> - **L4 手工**：需要人工决策/观察的语义确认
>
> 状态标记：✅ 已自动化 ｜ ✅-L3 验收脚本已验证（脚本资产 `.verify/v2/`，日志 `.verify/v2/logs/`）｜ ⬜ 计划中
>
> 引擎仅支持组形态白名单：未列入 `tables` 的表完全不同步，也没有新表自动接入；
> 相关用例按此语义表述（「白名单外表」= 未列入 tables 的表）。

## 0. 测试缺口根因分析（架构复盘）

| 根因类别 | 说明 | 暴露的缺口 | 对策（已落地） |
|---------|------|----------|---------------|
| 路径未覆盖 | 集成测试显式传了非默认参数，默认路径从未被容器执行 | 全默认参数路径（target_database 同名默认、stock 多表并行） | 新增同名默认 + 多表并行集成（§1 DATA-INT-01/02） |
| 时序类 | 失效需要「外部组件状态变化」触发，容器测试都是稳态 | etcd 分区、下游瞬断等故障时序 | 新增 etcd 停机时序集成（§2 HA-INT-01）；重试分类单测（§6 MQ-UNIT-02/03） |
| 组合态缺失 | 多配置项组合（use_gtid × rotate × 重启）从未同测 | GTID × rotate × 重启组合态 | 新增 GTID rotate 集成（§3 SRC-INT-02） |
| 竞态窗口 | 挂起/审批/外部操作交叠的窗口态 | DDL approve 竞态窗口 | 新增过期 DDL 放行集成（§4 DDL-INT-02） |
| 组件层不可达 | bug 位于 cmd 装配层闭包，internal 集成测试不可达 | cmd 装配层闭包的运行时行为（DDL diff/verify） | L3 真实验收覆盖（装配层只有端到端能测——教训：**关键默认路径必须有一条端到端**） |

**架构教训**：集成测试必须覆盖 ① 全默认参数路径 ② 外部组件故障时序 ③ 配置组合矩阵 ④ 挂起/审批竞态窗口。

## 1. 数据一致性（DATA）

| ID | 场景 | 预期 | 层级 | 自动化 | 状态 |
|----|------|------|------|--------|------|
| DATA-01 | 全量 stock：启动前预插数据（多库多表） | 目标 COUNT=源；done 标记落盘 | L2 | TestIntegrationStock | ✅ |
| DATA-02 | **同名 target_database 默认 + 3 表并行 stock** | 全表 done、行数收敛 | L2 | TestIntegrationStockParallelSameName | ✅ |
| DATA-03 | 增量全类型 DML（24 列：tinyint~bigint unsigned max/decimal(12,4)/float/double/bit/char/varchar/text/blob/varbinary/datetime(3)/timestamp/date/time/year/enum/set/json/NULL/负值/emoji） | INSERT/UPDATE/DELETE 收敛，verify 0 | L2+L3 | TestIntegrationPipelineE2E + 验收 workload1 | ✅ |
| DATA-04 | 主键变更（UPDATE PK） | 目标旧行无残留 | L2+L3 | TestIntegrationPipelineE2E + 验收 B4 | ✅ |
| DATA-05 | 联合主键表 DML + 批量删除 | 收敛无残留 | L2+L3 | mysql sink 集成 + 验收 B5 | ✅ |
| DATA-06 | 5000 行大事务 | 行数一致、位点推进事务尾 | L3 | 验收 B7 | ✅-L3 |
| DATA-07 | 同行 25 次高频 UPDATE 顺序性 | 终值一致 | L3 | 验收 B10 | ✅-L3 |
| DATA-08 | mask 脱敏列（phone → 139\*\*\*\*0012） | 目标为脱敏值；verify 对被脱敏表必然 DIFF（语义限制，见 §9 注 6） | L2+L3 | transform 单测 + 验收 B9 | ✅ |
| DATA-09 | 白名单未列出的表不同步 | 目标无该表数据；其 DDL 被过滤 | L3 | 验收 B1/E6 | ✅-L3 |
| DATA-10 | do_stock=false 只增量 | 预插不同步、增量同步 | L3 | 验收 F4 | ✅-L3 |
| DATA-11 | uint64 极值回绕重解释（go-mysql 有符号交付） | 18446744073709551615 正确落目标 | L2 | TestIntegrationPipelineE2E | ✅ |
| DATA-12 | verify 对账：一致/注入差异/逐列检出 | mismatch 语义正确、diffs 可读 | L2+L3 | TestIntegrationCompareTable + 验收 B8 | ✅ |
| DATA-13（反方） | 无主键表参与同步 | stock/启动核对 fail-fast 指明「无主键无法寻址」 | L2+L3 | §7 对抗验证 ADV-01 | ✅-L3 |
| DATA-14（反方） | 目标表额外 UNIQUE KEY（非主键 UK） | upsert ON DUPLICATE KEY 命中 UK 路径不炸 | L3 | §7 ADV-02 | ✅-L3 |
| DATA-15（反方） | 保留字表名/列名（反引号转义） | 全链可用 | L3 | §7 ADV-03 | ✅-L3 |
| DATA-16（反方） | 零日期 0000-00-00（sql_mode 放开） | on_convert_error 默认 null 策略不炸 | L3 | §7 ADV-04 | ✅-L3 |
| DATA-17（反方） | 64KB 超长行（TEXT 顶格） | MySQL/MQ 双写不截断不失败 | L3 | §7 ADV-05 | ✅-L3 |
| DATA-18（反方） | 目标 AUTO_INCREMENT 漂移（显式 id 插入） | 持续同步不冲突 | L3 | §7 ADV-06 | ✅-L3 |
| DATA-19（反方） | 大小写变体表（容器 Linux 区分大小写，`shop.Orders` vs `orders`） | 归一命中：变体表 INSERT/DELETE 收敛进小写白名单表、不暂停、目标不建新表。**运维红线：同库异结构变体表=禁区**（结构经 information_schema ci collation 命中，异结构会列数错配） | L3 | 验收 95 DATA-19 | ✅-L3 |
| DATA-20（反方） | 事务中途 DDL（BEGIN; INSERT; ALTER; INSERT; COMMIT，隐式提交边界） | 行事件-DDL barrier-行事件三段收敛，DDL auto 同步，verify 0 mismatch | L3 | 验收 95 DATA-20 | ✅-L3 |

## 2. 位点（POS）

| ID | 场景 | 预期 | 层级 | 自动化 | 状态 |
|----|------|------|------|--------|------|
| POS-01 | kill -9 大事务中段（50k 行，位点节流窗口内） | 重启重放幂等收敛、零重复 | L3 | 验收 C2 | ✅-L3 |
| POS-02 | 位点回拨（停机 → etcdctl/Web SET-POSITION → 重启） | 重放幂等收敛；**运行中回拨会被水位终值覆盖（必须停机）** | L3 | 验收 C3 | ✅-L3 |
| POS-03 | binlog PURGED 检测 | 位点文件早于现存最老 → 拒启增量 | L2+L3 | TestIntegrationStock(PURGED) + 验收 C5 | ✅ |
| POS-04 | follower/旧 leader 位点写被拒（lease 门槛） | 写入 ErrGateRejected | L2 | TestIntegrationStoreGate / TestIntegrationElector_Takeover | ✅ |
| POS-05 | 位点不超前不变式（乱序 ack 万级并发） | base 只在连续确认后推进 | L1 | TestWatermark_NeverAhead / ConcurrentAcks | ✅ |
| POS-06 | GTID 模式续传（use_gtid=true） | GTID 集与 master 一致、重启续接 | L3 | 验收 C6 | ✅-L3 |
| POS-07 | **GTID rotate 保留集合** | rotate/XID 事件集合非空单调、内部跟踪初始化 | L2 | TestIntegrationSourceGTIDRotate | ✅ |
| POS-08 | GET /position 实时反映 etcd | 写入后 name/pos/seq 前进 | L3 | 验收 C1 | ✅-L3 |
| POS-09 | GTID/file:pos 位点迁移（Web migrate-gtid / migrate-filepos，P3） | 决策矩阵+门禁；正向=熔断态迁移受理（gap≤门槛）→重启续流；反向=PURGED 缺口态复位最老文件头→重放收敛零重复 | L2+L3 | TestPositionMigrate*（决策矩阵+路由守卫+typed-nil 回归） + 验收 95 POS-09 | ✅ |

## 3. 高可用（HA）

| ID | 场景 | 预期 | 层级 | 自动化 | 状态 |
|----|------|------|------|--------|------|
| HA-01 | 双节点竞选单 leader | 恰一 leader、单 Binlog Dump、term 单调 | L2+L3 | TestIntegrationElector_Takeover + 验收 D1 | ✅ |
| HA-02 | kill -9 leader 接管 | ≤TTL+10s；窗口数据收敛 | L2+L3 | TestIntegrationElector_Takeover + 验收 D3 | ✅ |
| HA-03 | **etcd 宕机 → OnLost 时延上界** | 停 etcd 后 8s 内失位；恢复自动重选 | L2 | TestIntegrationElector_EtcdOutageOnLostTiming | ✅ |
| HA-04 | etcd 宕机端到端（管线停=安全态，窗口写入不投递，恢复补齐） | 单点+集群均验证 | L3 | 验收 D6 | ✅-L3 |
| HA-05 | 旧 leader 回归降 follower | 仍单 Dump 连接 | L3 | 验收 D4 | ✅-L3 |
| HA-06 | resign 优雅让位 | 0.6~1s 切换 | L3 | 验收 D5 | ✅-L3 |
| HA-07 | 源库宕机→恢复 | 断连退避重连、续传零丢失 | L3 | 验收（源库 blink） | ✅-L3 |
| HA-08 | 目标库宕机→恢复（ErrInvalidConn 重试） | 持续重试不退出、恢复收敛 | L3 | 验收（目标库 blink ×2） | ✅-L3 |
| HA-09（反方） | 全量进行中 kill leader → 新 leader 接管 stock | session_id 不同 → 整表重扫，最终收敛 | L3 | §7 ADV-07 | ✅-L3 |
| HA-10 | 全量失败 fail-fast（无快照锁权限） | 拒绝无感降级、不启管线 | L3 | 验收（RELOAD 授权前） | ✅-L3 |
| HA-11 | 下游不可达拒绝启动 | fatal「下游目标不可达」 | L3 | 验收 A7 | ✅-L3 |
| HA-12（反方） | 重复 approve/skip 同一 DDL 幂等 | 二次调用返回成功（Settled 短路） | L3 | §7 ADV-08 | ✅-L3 |
| HA-13 | **人工暂停/恢复管线**（13 §3，2026-11-01 新增）：pause 受理→paused+`pipeline_paused_by_op`+保持 Leader；暂停期零投递、位点停摆；resume 从 etcd 位点续跑、积压收敛；**标志跨重启/失位持久**（本任期不启动，直到显式 resume）；重复 pause/resume 被 CAS 拒绝 | 全链语义正确 | L1+L3 | TestPausePipeline_* / TestResumePipeline_*（护栏） + 验收 55 PP | ✅ |

## 4. DDL 自适应与审批（DDL）——表变化零人工/仅 Web 审批语义

| ID | 场景 | 预期 | 层级 | 自动化 | 状态 |
|----|------|------|------|--------|------|
| DDL-01 | auto：ADD COLUMN 自动同步+行携新列 | 零干预收敛 | L2+L3 | TestIntegrationDDLPolicies + 验收 E1 | ✅ |
| DDL-02 | auto：varchar 扩容 | 自动同步、长值无损 | L2+L3 | TestIntegrationDDLPolicies + 验收 E2 | ✅ |
| DDL-03 | 白名单外（DROP）→ pause 挂起 → skip | 目标保旧结构、继续同步、审计 | L2+L3 | TestIntegrationDDLPolicies + 验收 E3/E5 | ✅ |
| DDL-04 | 默认 pause + 白名单内 → Web approve 自动执行 | 一次点击全自动（含新列数据流动） | L3 | 验收 E4 | ✅-L3 |
| DDL-05 | CREATE 白名单外表 → 过滤（组形态下无新表自动接入） | 无挂起、目标不建表、数据不流、管线不停 | L3 | 验收 E7' | ✅-L3 |
| DDL-06 | **approve 竞态：挂起期间源表被删/改名** | 过期放行（audit skip）、管线不死、后续流动 | L2 | TestIntegrationDDLExpiredDuringPause | ✅ |
| DDL-07 | 白名单外表 DDL 过滤 | 管线不停 | L3 | 验收 E6 | ✅-L3 |
| DDL-08 | RENAME 白名单外表 | 忽略不挂起（新表名同样不在白名单） | L3 | 验收 E8 前半 | ✅-L3 |
| DDL-09（反方） | TRUNCATE TABLE | 白名单外 → pause 审批 | L3 | §7 ADV-09 | ✅-L3 |
| DDL-10 | 集群下 DDL 审批（follower API → leader 执行） | gate 走 etcd 任意节点可批 | L3 | 验收 E9 | ✅-L3 |
| DDL-11 | pause 挂起期间位点冻结（水位不越 DDL 位点） | 挂起期写入审批后补齐 | L2+L3 | TestIntegrationDDLPolicies + 验收 E3 | ✅ |
| DDL-12（反方） | 注释变更（仅 COMMENT，表+列） | ddlgen 白名单差异（列注释/表注释）→ auto 零干预同步、无挂起、数据续流 | L3 | 验收 95 DDL-12 | ✅-L3 |

## 5. 启动/配置（BOOT/CFG）

| ID | 场景 | 预期 | 层级 | 自动化 | 状态 |
|----|------|------|------|--------|------|
| BOOT-01 | flag 契约（-version/缺参/-h） | exit 语义正确 | L3 | 验收 A1 | ✅-L3 |
| BOOT-02 | 坏配置 17 处错误不短路 | 完整清单 exit=1 | L2 | config 表驱动 + 验收 A2 | ✅ |
| BOOT-03 | ${ENV} 缺失指名报错 | exit=1 | L2+L3 | config 测试 + 验收 A3 | ✅ |
| BOOT-04 | binlog_format 非 ROW 拒启 | fail-fast | L3 | 验收 A4 | ✅-L3 |
| BOOT-05 | 目标缺列 fail-fast / exclude 列 NOT NULL 无默认 fail-fast | 启动核对 | L3 | 验收 A5a | ✅-L3 |
| BOOT-06 | 下游不可达拒启 | fatal | L3 | 验收 A7 | ✅-L3 |
| BOOT-07 | /config 密码打码 | 全部 *** | L3 | 验收 A8 | ✅-L3 |
| BOOT-08 | etcd 配置源（-config-key 无 -config-file） | 正常加载 | L2 | config fake etcd + loadConfig 集成路径 | ✅（L3 未单跑——见 §9 注 7） |
| CFG-01 | /rules/effective 合并视图（表级覆盖/库级继承） | 每表生效规则正确 | L3 | 验收 F5 | ✅-L3 |
| CFG-02 | 配置不热更（改 etcd 须重启） | 运行中无感 | L3 | 验收 F6/集群滚动重启 | ✅-L3 |
| CFG-03（反方） | auth_disabled=true 显式裸奔 | 放行 + 配置校验 | L3 | §7 ADV-10 | ✅-L3 |

## 6. API（API）

| ID | 场景 | 预期 | 层级 | 自动化 | 状态 |
|----|------|------|------|--------|------|
| API-01 | 认证矩阵（无凭据/错密码/正确） | 401/401/200 | L2+L3 | router 测试 + 验收 G1 | ✅ |
| API-02 | confirm 门禁全覆盖（restock/verify/approve/skip/resign） | 无 confirm 400 | L2+L3 | router 测试 + 验收 G2 | ✅ |
| API-03 | 位点危险操作 SET-POSITION 门禁 | 错 confirm 400、正确生效；**只覆盖 name/pos，gtid_set/seq/任期以 etcd 现值为底沿用**（ADV-16 回归：整值覆盖抹空 GTID 集=恢复即熔断） | L3 | 验收 G3 + TestMergeSetPosition + 验收 55 PP-07 | ✅ |
| API-04 | 审计链路（who/what/result + etcd events/audit） | 全写操作留痕 | L3 | 验收 G4 | ✅-L3 |
| API-05 | restock 清标记+重启精准重跑 | 只重跑目标表 | L3 | 验收 G5 | ✅-L3 |
| API-06 | /verify 契约（缺参 400/错 pk 500 明细/差异 200） | 语义正确 | L3 | 验收 G6 | ✅-L3 |
| API-07（反方） | 未知路由 404 / 方法不匹配 405 | gin 默认语义不 5xx | L3 | §7 ADV-11 | ✅-L3 |
| API-08 | /status 管线可见性（pipeline_state/pipeline_error/event_lag_seconds，P0） | 五态流转正确、paused 带 error、契约字段齐；**人工暂停透传 `pipeline_paused_by_op=true`，致命错误暂停时字段省略**（前端据此切换「恢复管线」入口） | L2+L3 | TestRouter_StatusPipelineFields + TestPipeline_SourceInvalidPositionFatal + 验收 A6 输出 | ✅ |
| API-09 | **pipeline pause/resume 门禁与审计**（13 §3，2026-11-01 新增） | 无 confirm 400 且闭包不被调；confirm 受理恰调一次；未装配 500；闭包错误 500 带消息；pause/resume 写 `pipeline.pause`/`pipeline.resume` 审计（Web /audit + etcd events/audit 双通道） | L1+L3 | TestRouter_PipelinePauseResume / NotWired / PauseError + 验收 55 PP-01/05/09 | ✅ |

## 7. 反方对抗验证记录（ADV，2026-10-29/30 真实环境执行）

> 方法：数据库专家/架构师从「配置错的、数据怪的、操作乱的」三个方向构造对抗输入，
> 在 WSL 真实环境逐个执行并记录。结果全部回填上方矩阵。

| ADV | 对抗输入 | 预期 | 实际结果 |
|-----|---------|------|---------|
| ADV-01 | 无主键表建表+插数+接入同步 | fail-fast 指明无主键 | ✅ 双层拦截：目标未预建→「不存在」拒启；补建后→「无主键且未配 order_by_column 无法寻址」拒启（05 §5.1.3） |
| ADV-02 | 目标表加额外 UNIQUE KEY 后 UPDATE 同键 | upsert 走 UK 更新不炸 | ✅ UPDATE 正常收敛（ON DUPLICATE KEY 撞 UK 路径无异常） |
| ADV-03 | 表名/列名用保留字 `order`/`key`/`select`（反引号） | 全链可用 | ✅ 建表+INSERT/UPDATE 全链收敛（2 行、max(select)=99） |
| ADV-04 | 零日期 0000-00-00 00:00:00（sql_mode 放开） | on_convert_error=null 不炸、目标 NULL | ✅ 目标 `<NULL>`，管线不受影响 |
| ADV-05 | 60KB 超长 TEXT 行（65536 超 TEXT 上限先在源端被拒——输入构造教训） | 双写完整 | ✅ 60000 字节两端一致 |
| ADV-06 | 显式大 id（999999）后普通 id 继续插入 | 目标 AUTO_INCREMENT 漂移不阻塞后续 | ✅ 后续行（id=34）正常同步 |
| ADV-07 | restock 清标记 + kill leader → 接管/重启后跨会话重扫 | 无 done 标记的新 leader 全量重扫收敛 | ✅ 重扫 1201 行=源、distinct 无重复（跨 session_id 整表重扫语义） |
| ADV-08 | 同一 DDL 重复 skip ×2 | 幂等成功（200） | ✅ 二次调用 200，Settled 短路 |
| ADV-09 | TRUNCATE TABLE | pause 审批 | ✅ 挂起等决策，skip 后恢复 |
| ADV-10 | auth_disabled=true + 无凭据访问 | 200 放行 | ✅ 修复配置校验 bug 后：无凭据 GET 200；confirm 门禁仍独立强制（400）——**修复：auth_disabled=true 时凭据校验豁免此前不可达（validate.go）** |
| ADV-11 | 未知路径 / 错误方法打 API | 404 无 5xx | ✅ 404/404/400 |
| ADV-12 | 位点文件名指向不存在文件 | PURGED 检查拒绝 | ✅（C5 同路径已验证） |
| ADV-13 | **位点回拨后重放窗口含已放行 DDL/已删表**（时间机器场景，对抗推演自 HA-04+POS-02 组合） | 不死锁 | **抓到 2 个真 bug 已修复**：① column_count_mismatch 无视 on_convert_error 策略强制 fatal → 改走三策略（null/zero 降级单列+计数告警，fail_event 保留上抛）；② 表已 DROP 后重放其历史行事件 fatal → 改跳过+告警+ack（必须 ack 否则水位洞）。修复后重放窗口自愈、管线存活 |
| ADV-14 | restock 后无人重启（leader 不重扫） | 符合文档语义（清标记→重启→重跑） | ✅ 行为符合 RUNBOOK §2；未重启则位点继续增量（清标记本身不触发重扫）——运维注意 |
| ADV-15 | **migrate-gtid/filepos 成功路径**（2026-10-31 新增 L3 组95 首轮抓到） | 迁移受理 200、位点写入 | ❌→✅ **抓到真 bug（typed-nil panic）**：plan*Migration 成功返回 nil `*apiError` 直接作 error 传 audit → 非 nil 接口包 nil 指针 → `result.Error()` 空指针 panic → HTTP 500 空响应、位点未写（gin Recovery 吞栈只留日志）。修复=apiErrToError nil 归一（两调用点）+ 回归测试 TestAuditTypedNilSafety。失败路径不受影响（非 nil verr 正常 400） |
| ADV-16 | **GTID 模式暂停态 SET-POSITION**（2026-11-01 组55 PP-07 设计时对抗推演抓到） | 改位点只覆盖 name/pos，gtid_set/seq/任期沿用现值（与 etcdctl 直写指引语义一致） | ❌→✅ **抓到真 bug（整值覆盖抹空 GTID 集）**：setPosition 只构造 {name,pos,node_id} 直接 Save（整值覆盖）→ use_gtid=true 下暂停态改位点把 gtid_set 写空 → resume 后管线必因「位点缺 gtid_set」熔断；运行态等值覆盖（G3）因 3s 内水位回写未暴露。修复=mergeSetPosition 融合决策（现值为底只覆盖 name/pos；现值读取失败拒绝盲写）+ 回归测试 TestMergeSetPosition；L3 PP-07 固化「改位点→gtid_set 保留→恢复采用续跑」 |
| ADV-17 | **审计 etcd key 真身路径**（2026-11-01 组55 PP-09 首轮抓到） | 环形审计 key = `{prefix}/events/audit`（NewAuditStore 契约：prefix=集群前缀） | ❌→✅ **抓到装配缺陷（prefix 双重拼接）**：admin.go 传 `coord.prefix+"/events/audit"` → 真身 key 变 `{prefix}/events/audit/events/audit`——环形读写自洽（Web /audit 正常）但 etcd 侧运维观察路径与文档/etcdctl 直觉不符；G4 的 `--prefix grep` 歪打正着命中双重 key 一直假阳性。修复=装配点传集群前缀 + PP-09 按真身 key 断言双通道留痕 |

## 7.5 多源（MS，2026-11-02 多源特性，PRD docs/prd/08 §2）

单进程多源（`sources[]` 配置 v2）的等价性验收：多源化的每个源在数据正确性/
全量增量/HA/投递语义/DDL 审批/暂停恢复/位点操作上与单独部署一个单源集群一致。
L3 脚本 `.verify/v2/35_ms_core.sh`（数据面）+ `46_ms_ha.sh`（故障面），
环境含第二源库 vf-src2 / 第二目标库 vf-dst2（00_reset.sh 重建）。

| ID | 场景 | 预期 | 层级 | 自动化 | 状态 |
|----|------|------|------|--------|------|
| MS-01 | 双源端到端一致性（双写 UPDATE/DELETE/PK 变更 → 聚合目标逐行一致） | 聚合目标数据与各源单独部署一致；逐源 verify mismatch=0 | L2/L3 | 35 组（E1） | ✅-L3 |
| MS-02 | 双源同时首启全量并发 → 交接正确 | 两源各自快照完成、增量起点正确、零丢失 | L2 | 35 组（E2） | ✅-L3 |
| MS-03 | 全量中杀进程 → 重启重扫/续扫 | 会话失配表整表重扫、done 表不重跑；终态收敛 | L2/L3 | 46 组（E3） | ✅-L3 |
| MS-04 | 某源源库宕机隔离与恢复 | 仅该源 failed+退避重试，其余源无中断；恢复后自动加入续传（D5） | L3 | 46 组（E5） | ✅-L3 |
| MS-05 | 跨源 DDL 冻结半径 | 源 A DDL pending 只冻结源 A，源 B 继续推进；approve 走 /sources/{sid}/ddl；记录带 source 字段（E6） | L2 | 35 组 | ✅-L3 |
| MS-06 | 逐源 pause/resume + 暂停期增量冻结/补投 | 暂停只影响该源；恢复后积压补投（E7） | L2 | 35 组 | ✅-L3 |
| MS-07 | 逐源位点回拨重放 | 回拨只影响该源；重放幂等收敛 verify=0（E8） | L2 | 35 组 | ✅-L3 |
| MS-08 | 逐源 restock | 只重扫该源该表，其他源与其他表不动（E9） | L2 | 35 组 | ✅-L3 |
| MS-09 | MQ 双源：topic 不相交（V4 拦死）+ envelope source 字段 + 各自保序 | 前缀不同不冲突；消息带 source=src-a；(topic,event_id) 去重成立（E10） | L2 | 35 组 | ✅-L3 |
| MS-10 | 背压隔离（分目标库：停 dst → 仅 src-a 反压/failed，src-b 不受影响） | 无背压传导；dst 恢复后 src-a 自愈（E11） | L3 | 46 组 | ✅-L3 |
| MS-11 | 配置校验 V1~V6：源数上限/id 规则/v1-v2 双写/跨源目标冲突/server_id 冲突/继承合并 | 全分支拒启或通过；v1 自动包装 default | L1+L3 | config 表驱动（multi_source_test.go）+ 35 组脚本分支 | ✅ |
| MS-12 | v1 配置零修改升级 + etcd 键迁移幂等 + 位点连续 | 自动包装 default；旧键 CAS 搬到 sources/default/；完成标记；二次启动不重搬；增量续传（E12） | L2 | 46 组 | ✅-L3 |
| MS-13 | 旧 leader 位点写拒绝（协调面失联 → gate 消失 → 熔断） | 双源管线停（安全态）；恢复重启后零丢失续传（E13） | L3 | 46 组 | ✅-L3 |
| MS-14 | 源级路由：旧顶层路由已移除（一律 404）/ /sources 清单 / 审计 source 字段 / 未知 sid 404 / restock 409（standby） | 旧路径 404（PRD 06 §1.4）；源级操作走 /sources/{sid}/... | L1+L3 | api multi_source_test.go + 35 组 | ✅ |
| MS-15 | server_id 按 node_id:sid 派生互异；schema 缓存/transformer 按源实例隔离 | 双 dump 连接并存不互踢；双源持续同步 | L1+L3 | identity 单测 + 35 组 | ✅ |
| MS-16 | 指标 label：源私有指标带 source、cluster 级（leader/etcd）不带；gocdc_sources/gocdc_source_up | PRD 07 §1 全清单 | L1+L3 | metrics 单测 + 35 组 | ✅ |
| MS-17 | 旧断言适配：源级失败不再进程退出（A4/A5a/A7 改源 failed(startup)+存活+退避）；etcd 直写键适配 v2（position/stock 下沉 sources/default/） | D5 语义变化如实反映在既有组 | L3 | 10/30/65/70 组适配 | ✅-L3 |

**MS 轮实证抓到的真问题（已修复）**：
1. **SourceDeps 静态字段 nil panic**（35 组首轮）：Web 启动与源装配并发，构造期快照的
   Gate/Rules/DDLLister 为 nil 且永不更新 → DDL approve nil deref panic（gin Recovery 兜 500）。
   修复=改惰性闭包 + handler 409 nil-guard（PRD 06 §1.5 边角同类）。
2. **leader 流程早退**（P2 冒烟）：runLeaderFlow 启动完源流程即返回，defer 立即清
   leading/leadCtxRef → 延迟就绪源与 pause→resume 永远无法启动。修复=leader 流程阻塞至
   任期结束（任期态存活整届）。

## 8. MQ sink（MQ）

| ID | 场景 | 预期 | 层级 | 自动化 | 状态 |
|----|------|------|------|--------|------|
| MQ-01 | RocketMQ 全量双写（FIFO topic） | 快照事件全到达 | L3 | 验收 I0 | ✅-L3 |
| MQ-02 | 信封格式（event_id/pk/before/after/position） | 自描述可对账 | L3 | 验收 I1 + rmq_compare.py | ✅-L3 |
| MQ-03 | 增量三方对账（源=目标=消费态） | 746 事件去重 622 行一致 | L3 | rmq_compare.py | ✅-L3 |
| MQ-04 | broker 宕机双写单边语义（MySQL 照写位点冻结 RMQ 重试） | 恢复自动补投 | L3 | 验收 I3 | ✅-L3 |
| MQ-05 | 崩溃重放幂等（84 重复按 event_id 去重） | 消费端去重收敛 | L3 | 验收 I4 | ✅-L3 |
| MQ-06 | 集群单产出（单 leader 恰一份） | 10 写=10 消息；切换后 100=100 | L3 | 验收 I5 | ✅-L3 |
| MQ-UNIT-01 | topic 命名（Kafka/RocketMQ 统一 {db}_{table}，点号一律不收；override 优先，覆盖值/topic_prefix 启动校验同样拒点） | 统一命名 + 拒点校验 | L1 | TestTopicName + config 表驱动 | ✅ |
| MQ-UNIT-02 | 错误分类：gRPC 瞬态可重试/配置类永久 | 错误分类正确性 | L1 | TestIsRetryable | ✅ |
| MQ-UNIT-03 | ErrInvalidConn 可重试（目标库闪断） | 闪断重试正确性 | L1 | TestIsRetryable | ✅ |
| MQ-UNIT-04 | kafka sink 收发（confluent-local 容器；fixture 即用带点库名） | key/envelope 到达 `cdc_edu_mall_admin_admin_user` | L2 | TestIntegrationKafkaSink | ✅ |
| MQ-UNIT-05 | **带点库名**（`edu.mall.admin` 是库名、`admin_user` 是其下表，全名 edu.mall.admin.admin_user——多源库按 . 聚合命名） | Kafka/RocketMQ topic 统一折叠为 `cdc_edu_mall_admin_admin_user`（库名内点→下划线，broker 字符集不收点，否则 RMQ 报 ILLEGAL_TOPIC）；RMQ 顺序组 key 同步折叠（`edu_mall_admin_admin_user_1`，折叠非单射只会并组串行=保序方向安全） | L1+L2 | TestTopicName + TestSend_DottedDB_TopicAndGroup + TestIntegrationKafkaSink | ✅ |
| MQ-12（反方） | 消息超限 OversizeError（>max_message_bytes，kafka sink） | 小消息阳性对照到达 → 大行预检 poison → PAUSED 带 OversizeError、进程存活 → 配置提限+重启 → 重放通过收敛 | L2+L3 | kafka sink 单测 + 验收 95 MQ-12（临时 v2-kafka 容器） | ✅ |

## 9. 已知语义限制（非缺陷，验收时勿当 bug）

1. verify 不感知 mask 列——被脱敏表对账必然 DIFF；干净表 0 mismatch 才是验收口径。
2. verify 复合主键表需显式传单列 `pk`；缺省 `pk=id` 在无 id 列表上返回明确 500。
3. 优雅停机用水位终值覆盖手工位点——回拨必须停机后做。
4. 白名单外 DDL approve 不自动执行（无安全生成器）——人工先改目标，approve=复验+resume。
5. RENAME 白名单外表 → 新旧表名均不在白名单（不同步）。
6. 启动核对要求规则覆盖表在目标预建（含 pause 策略下 CREATE 可审批自动建的新表）——运维预建库即可。
7. RMQ topic 须预建且 FIFO 属性（`mqadmin updateTopic ... -a +message.type=FIFO`）；v5 客户端只拉路由不建 topic。
8. event_lag_seconds 是活动口径（无新事件时增长=正常）；追平判定看 master_position_lag_bytes=0。
9. restock 只清标记，重跑需重启/接管触发（RUNBOOK §2）；运行中的 leader 不会自发重扫。
10. 位点回拨后的重放窗口若包含「已放行的 DDL/已删表」：历史行像按 on_convert_error 策略降级转换
   （缺失尾列置 null/zero+计数告警）、已删表的行事件跳过放行——管线自愈不 fatal（ADV-13）。
11. auth_disabled=true 是显式裸奔豁免：无凭据可访问 Web，但 confirm 门禁仍独立强制（ADV-10）。
12. **人工暂停是持久意图**（13 §3）：标志存 etcd 无 lease，跨重启/失位保持，直到显式
   resume；热备节点调用 pause/resume 被拒（仅 Leader 承担管线）。恢复从 etcd 位点
   重启——暂停期间修改的位点会被采用；Web SET-POSITION 只覆盖 name/pos，其余字段
   沿用现值（ADV-16）。
13. **MQ topic 统一命名不收点**（Kafka/RocketMQ 同一套 `{db}_{table}`，见 MQ-UNIT-01/05）：
   库名自身含点（如 edu.mall.admin）一并折叠为下划线；topic_prefix 与规则级覆盖 topic
   启动校验同样拒点——旧配置 `cdc.` 前缀/覆盖值会被启动校验拒绝，须迁移为 `cdc_` 形式，
   broker 侧需同步预建新名 topic。

## 10. 集成测试套件总览（`go test -tags=integration ./...`，testcontainers）

| 包 | 测试 | 覆盖 |
|----|------|------|
| etcdx | TestIntegrationEtcdX | client/KV 重试/lease/注册 |
| position | TestIntegrationStoreGate | lease 写门槛（follower 拒写） |
| schema | TestIntegrationMySQLLoader | 全类型结构加载/版本化 |
| source | TestIntegrationSource；**TestIntegrationSourceGTIDRotate** | binlog 收发/优雅停机；GTID rotate 集合保持 |
| sink/mysql | TestIntegrationMysqlSink* | 批量幂等/主键变更/联合主键/缺列 fail-fast/DDL diff |
| sink/kafka | TestIntegrationKafkaSink | 真实 broker 收发/key 断言（fixture 用带点库名 edu.mall.admin） |
| pipeline | TestIntegrationPipelineE2E；TestIntegrationDDLPolicies；**TestIntegrationDDLExpiredDuringPause**；**TestPipeline_SourceInvalidPositionFatal**（P0 拉取侧静默死亡回归）；**TestIntegrationDDLGateGC** | 全链 E2E/kill 重放；DDL 三策略；过期 DDL 放行；位点非法熔断；已决 DDL 7 天 GC |
| stock | TestIntegrationStock；**TestIntegrationStockParallelSameName**；**TestIntegrationStockTablesExceedWorkers**；**TestIntegrationMarkerGC** | 并发写入全量/重扫/PURGED；同名默认+多表并行；表数>workers 槽位独占；死标记 GC |
| api | **TestIntegrationAuditRing**（P2 CAS 环形 100）；**TestIntegrationAuditLegacyMigration**（P2 旧散 key 合并） | audit 单 key 并发写不丢；旧格式一次性迁移 |
| elector | TestIntegrationElector_Takeover；**TestIntegrationElector_EtcdOutageOnLostTiming** | 选举/接管/门槛；etcd 停机失位时延 ≤8s |
| verify | TestIntegrationCompareTable | 对账/差异明细 |

容器镜像可用环境变量覆盖：`MYSQL_IMAGE`（默认 mysql:8）、`GOCDC_TEST_ETCD_IMAGE`（默认 cnb.cool etcd-3.5.16）。
