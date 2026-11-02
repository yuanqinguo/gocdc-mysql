# 06 · API 与 Web 控制台

## 1. API 设计

### 1.1 原则

- **源是一等路由段**：源私有资源的读写全部挂在
  `/api/v1/sources/{sid}/...` 下；cluster 级资源（status 摘要、审计、
  leader）保持原位。
- **路由一次性收敛**：源私有端点不保留顶层旧路由别名，单源（default）
  部署同样走 `/sources/default/...`——单一调用形态，不做弃用窗口。
- 鉴权（HTTP Basic + 恒时比较）、`confirm=true` 写门禁、审计、错误信封
  全部不变（`internal/api/router.go`、`auth.go`）。

### 1.2 端点清单（v2）

**只读：**

| 方法/路径 | 功能 | 与现行差异 |
|-----------|------|-----------|
| `GET /api/v1/status` | 节点/集群信息 + **全部源摘要数组** `sources[]`（每项：id、state、error、since、paused_by_op、event_lag、caught_up、use_gtid、position 摘要） | 顶层 cluster 字段不变；管线状态一律读 `sources[]`（顶层无镜像字段） |
| `GET /api/v1/sources` | 源清单：id、源/目标地址（打码）、规则数、表数、sink 集、状态摘要 | 新增（聚合总览页数据源） |
| `GET /api/v1/sources/{sid}/status` | 单源完整管线状态（= 现行 status 的管线部分 + lag） | 新增 |
| `GET /api/v1/sources/{sid}/position` | 该源位点 | = 现行 `/position` |
| `GET /api/v1/sources/{sid}/rules/effective` | 该源规则编译视图 | = 现行 `/rules/effective` |
| `GET /api/v1/sources/{sid}/ddl-events?schema=&table=` | 该源 DDL 审批列表 | 记录含 `source` 字段 |
| `GET /api/v1/ddl-events` | 全集群 pending 合并（带 source 字段） | 现行端点扩展（角标/聚合页用） |
| `GET /api/v1/config` | 打码后的完整 v2 配置 | 递归打码天然覆盖 sources[] |
| `GET /api/v1/audit` | 审计环 | 条目含 `source` 字段（可空） |

**写操作（全部 `confirm=true` + 审计）：**

| 方法/路径 | 功能 |
|-----------|------|
| `POST /api/v1/sources/{sid}/position` | 该源位点回拨（SET-POSITION 口令门禁不变） |
| `POST /api/v1/sources/{sid}/position/migrate-gtid` / `migrate-filepos` | 该源位点模式迁移 |
| `POST /api/v1/sources/{sid}/ddl/:event_id/approve` / `skip` | 该源 DDL 审批 |
| `POST /api/v1/sources/{sid}/rules/by-schema/:schema/:table/restock` | 该源单表重跑全量 |
| `POST /api/v1/sources/{sid}/pipeline/pause` / `resume` | 该源人工暂停/恢复 |
| `POST /api/v1/sources/{sid}/verify` | 该源抽样对账（body 增加 `source` 可省——路径已定） |
| `POST /api/v1/leader/resign` | 主动让位（cluster 级，不变） |

### 1.3 DTO 变更要点

- `StatusData.sources: []SourceStatus`（新增）；`SourceStatus` 复用现行
  `PipelineSnapshot` 字段集（dto.go:22-49）+ `id` + `position` 摘要。
- `DDLRecord`、`AuditEntry`、`VerifyRequest/Result` 增加 `source` 字段。
- 信封、错误码、`respondErrData`（verify 专用）不变。

### 1.4 旧顶层路由（已移除）

源私有端点的顶层旧路径（`/position`、`/rules/effective`、`/pipeline/*`、
`/verify`、`/ddl/:id/*`、`/restock` 等）不注册路由，一律 `404`；调用方
统一迁移到 `/sources/{sid}/...`。依据：项目未上生产、无存量调用方，
保留双形态只会让「同一操作两种 URL」长期共存，监控与脚本按 sid 显式
寻址反而更明确。cluster 级端点（`/status`、`/sources`、`/ddl-events`、
`/audit`、`/config`、`/leader/resign`）保持原位不变。

### 1.5 实现注意

- 现行 `Deps` 持单源闭包（`cmd/gocdc/admin.go:26-58`）改为
  `map[string]SourceDeps`（sid → 闭包集），路由中间件解析 `{sid}` 查表，
  未知 sid 返回 `404`；源集合在装配期固定（配置不热更 ⇒ 进程生命周期
  内不变，map 只读，无并发问题）。
- 现行已知边角顺带修复：standby 上 `POST /restock` 因 `stockSvc.Load()`
  为 nil 触发 panic 被 gin Recovery 兜成 500（PRD 调研发现）——v2 的
  SourceDeps 在非 leader 节点上未装配 stock 闭包时显式返回
  `409 "本节点当前不承担管线"`，与 pause 的现行行为对齐。

## 2. Web 控制台

### 2.1 信息架构：源作用域与 URL 设计

**源作用域是路由级状态，不是组件内部状态。** 顶栏新增**源选择器**
（AntD Select），全局作用域：所有页面的数据与操作随选中源切换；选中
值编码进 URL，刷新、书签、分享链接、浏览器前进后退都自然保持作用域。

**URL 承载方式：query 参数 `?source={sid}`**（而非把 `/sources/{sid}/`
塞进页面路径）。决策理由：

- 页面集合不变（现行六个路由照旧），路由表不加层级，`AppLayout`/
  导航/懒加载结构零重排；
- 「全部」= 无参数（`/`），单源 = `/?source=order-center`——**单源
  （v1 升级）部署的 URL 与现状完全一致**（默认无参数即唯一源）；
- API 路由已经用了 `/api/v1/sources/{sid}/` 路径段承载源，前端页面
  用 query 与之不冲突——API 面向程序（REST 层级语义优先），页面面向
  人（URL 稳定性优先）。

交互规则：

- 顶栏 cluster 级信息（集群名/节点/版本/API 在线灯/角色/任期）**不随
  选择器变**——它们是进程/集群属性，恒定展示。
- 切换源 = 同页路由内更新 query（react-router `setSearchParams`），
  页面组件按 sid 重新取数（react-query 缓存按 key 隔离，§2.4）。
- 从聚合视图点源卡片下钻 = 导航到 `/?source={sid}`；浏览器后退回到
  聚合视图。

### 2.2 逐页改造

**总控台 `/`（全部视图）——新增源总览**

布局自上而下三段：

1. **汇总条**：源总数 / 就绪数 / 告警数（failed+paused+DDL 等待）、
   待决 DDL 合计、cluster 级信息（leader 节点/任期/运行时长）+「主动
   让位」按钮（cluster 级操作放汇总条而非源卡——让位影响全部源，
   放在单源卡片会误导作用域）。
2. **多源拓扑图**：N 个源 MySQL 节点汇入中心 GOCDC 节点，再按各源
   实际启用的 sinks 扇出到下游节点（MySQL/Kafka/RocketMQ）。每个源→
   GOCDC 边用状态彩带同色小点标记健康度（绿=running、黄=追赶中、
   红=paused/failed）；点击源节点进入该源单源视图。N > 8 时折叠
   为「N 源汇聚」分组节点 + 悬浮展开。
3. **源卡片栅格**（每源一卡，响应式两列）：
   - 卡头：源 id + 状态彩带（running/stock/paused/failed + DDL 等待/
     追赶中/人工暂停叠加，沿用现行 DashboardPage 彩带分类逻辑）
   - 卡身：源地址 → 目标地址（多个 sink 逗号列举）、位点
     （`name:pos` + 更新时间）、event lag、master lag、规则/表数
   - 卡脚：操作区——暂停/恢复（该源，leader 校验同现行）、待决 DDL
     角标（点击跳 `/ddl?source={sid}`）、「详情」进入单源视图
   - **排序规则**：异常优先（failed > paused > DDL 等待 > 追赶中），
     同级按 event lag 降序——值班第一眼看到的是最需要处理的源
   - failed 源卡片：红色 + 错误摘要 + 重试中标识（距下次重试倒计时
     或最近失败时间），无操作按钮（装配期错误靠修复环境，非页面操作）

数据源：`GET /status`（已含全部源摘要，15s 轮询不变）+
`GET /sources`（规则/表统计、地址展示，staleTime 30s）。聚合视图**不
提供**批量危险操作（批量暂停/批量回拨）——单源单操作、单次 confirm
是现行门禁语义的一部分，聚合层只读 + 跳转。

**总控台（单源视图 `/?source={sid}`）**：现版布局原样保留（拓扑图、
指标格、暂停/恢复/让位按钮），四处微调——拓扑图源节点标注源 id 与源
地址；「主动让位」提示改为 cluster 级语义说明（让位会切换**全部**源，
确认弹窗列出受影响源数）；位点卡数据取自 `/sources/{sid}/status`；
页面标题与浏览器标签带源 id（多标签页值班场景区分）。

**复制规则 `/rules`**：单源视图沿用两级探索器（库→表），数据源改
`GET /sources/{sid}/rules/effective`；全部视图显示按源分组的折叠列表
（源头 + 该源库表计数），下钻进单源。restock/verify 操作带 sid
调用新路由。

**结构变更 `/ddl`**：表格加「源」列；全部视图合并展示全部源 pending
（按源分组排序），单源视图过滤该源。审批操作走
`/sources/{sid}/ddl/:id/approve|skip`，确认弹窗语义文案不变（白名单/
破坏性 DDL 指引）。

**位点管理 `/position`**：单源视图 = 现版（位点卡、模式迁移面板、
SET-POSITION 三栏危险表单、etcdctl 直写指引——指引中的 key 模板更新为
`/gocdc/{cluster}/sources/{sid}/position`）。全部视图只读展示各源当前
位点摘要 + 跳转单源操作（危险操作不允许在聚合视图批量执行——单源
单操作是现行门禁语义的一部分）。

**生效配置 `/config`**：JSON 浏览器展示完整 v2 配置（打码后）；
「规则视图（按库）」标签增加源维度（按 sources 分组的树）。

**审计 `/audit`**：表格加「源」列（cluster 级操作显示 —）。

**登录页**：流程图更新为「N 个源 MySQL → GOCDC（多源）→ 下游」示意，
与多源定位对齐（营销区，低优先级）。

### 2.3 取数与轮询（react-query）

| 数据 | Hook/来源 | 轮询 |
|------|-----------|------|
| 全部源摘要 | `useStatus`（一次拉全部） | 15s（不变） |
| 源清单统计 | `useSources` | staleTime 30s |
| 选中源位点/规则/DDL | 现行 hooks 改造为参数化 `usePosition(sid)` 等 | 15s/15s/15s/30s（不变） |

聚合视图不产生 N 倍请求（`/status` 一次返回全部）；源详情只在选中时
请求。全局默认（retry 1、staleTime 10s、refetchOnWindowFocus false）不变。

### 2.4 前端实现适配面

对 `web/src` 的改造清单（实现指引，非最终任务拆解）：

**类型层（`shared/api/types.ts`）**

- `StatusData` 增加 `sources: SourceStatus[]`；新增 `SourceStatus`
  （id + 现行 `PipelineSnapshot` 字段集 + position 摘要）。
- `DDLRecord`、`AuditEntry`、`VerifyResult` 增加 `source: string`。
- 新增 `SourceSummary`（`GET /sources` 响应：id、源/目标地址、规则数、
  表数、sink 集、状态摘要）。
- `StatusData` 只保留 cluster 级字段 + `sources[]`，无顶层管线镜像字段——
  前端与后端单一契约，管线状态一律读 `sources[]`。

**取数层（`shared/api/queries.ts`）**

- 现行 hooks 参数化：`usePosition(sid)`、`useDDLEvents(sid)`、
  `useEffectiveRules(sid)`、`useVerify(sid)`…；**query key 必须含 sid**
  （`['position', sid]`），否则切换源会命中缓存串数据——这是本改造
  最容易踩的实现坑，review 重点。
- 新增 `useSources`（源清单）、`useSourceStatus(sid)`；`useStatus` 一次
  拉全部源摘要供聚合视图与顶栏，不参数化。
- 轮询节奏不变（§2.3）；失效策略：源级写操作成功后只 invalidate 该
  源的 key + `useStatus`（聚合摘要同步）。

**组件层**

- 源选择器（顶栏，`AppLayout`）+ `useSourceScope()` hook（读
  `?source=`，提供 `currentSid | null`（null=全部）与 `setSid`），所有
  页面经此取作用域，禁止各页自行解析 query。
- 现行组件（RuleExplorer、位点卡、DDL 表格、SET-POSITION 表单、审计表）
  **原样复用**，外层由页面喂入「该源数据 + 带 sid 的操作回调」——
  组件内部不感知多源，改造面收敛在页面层。
- 聚合视图新增：源卡片、多源拓扑图（扩展现行 Topology 组件为
  N-in 形态）、汇总条。
- 导航角标：DDL pending 角标改为全部源合计数（`useStatus` 摘要），
  角标点击进 `/ddl` 全部视图。
- 移动端：源选择器收进顶栏抽屉（与现行导航抽屉同级交互）。

**测试层**

- `web-check`（typecheck + lint）与 `web-test` 全绿；关键单测：scope
  hook 的 query 解析/回退、query key 含 sid 的缓存隔离、聚合卡片排序
  规则、单源部署的选择器退化（仅一项时是否隐藏选择器——**隐藏**，
  单源部署不展示无意义的切换器）。

### 2.5 门禁与安全不变项

HTTP Basic 单凭据全权限、confirm 门禁、SET-POSITION 口令、危险操作二次
确认、Cookie 会话（7 天）、服务端打码——全部语义不变，仅作用域按源
切分。多源下「暂停/回拨/审批」等危险操作的审计记录必须带 source 字段
（§1.3），保证事后可追溯到源。
