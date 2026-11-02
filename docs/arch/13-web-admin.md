# 13 · Web 管理端设计

> 定位：API 优先的管理端——REST API 服务 curl/脚本运维，React 单页内嵌进二进制（go:embed）分发；看图归 Grafana，操作归 Admin。

## 1. 定位与原则

1. **API 优先**：先定义 REST API，前端只是消费者；API 同时服务 curl/脚本运维。
2. **看图归 Grafana，操作归 Admin**：Admin 不做图表系统，只做状态摘要 + 运维动作。
3. **默认安全**：HTTP Basic 单用户名密码鉴权默认开启；写操作二次确认 + 审计日志。
4. **单二进制**：前端构建产物 `go:embed` 进二进制，无静态文件部署。

## 2. 技术选型

| 层 | 选型 | 说明 |
|----|------|------|
| 后端 | gin（已有依赖）+ 中间件（auth/audit/CORS） | 与主进程同进程，读内存态 + etcd |
| 前端 | React 18 + TypeScript + Vite + Ant Design 5 | 国内管理台事实标准：表格/表单/描述列表组件最全（本系统 8 个页面全是表格+表单形态）、生态与招人面最广。**部署方式与 Vue 完全一致**——都是构建出静态 dist 由 `go:embed` 内嵌单二进制，选型不影响部署；若团队更熟 Vue，此规模（8 页面）换回无架构成本 |
| 图表 | 仅内嵌极简 sparkline（自绘 SVG） | 复杂图表引导去 Grafana |

```
web/            # 前端源码（独立 npm 工程；构建产物 embed 进 internal/api）
internal/api/   # API 实现（web/ = 前端源码，internal/api = 后端）
  ├── router.go       # /api/v1/*
  ├── handlers_*.go
  ├── auth.go         # HTTP Basic 单用户名密码（username/password，支持 ${ENV}）
  └── dist/           # embed 构建产物（web/ npm build 输出）
```

## 3. REST API 规范（`/api/v1`）

### 只读

| 方法/路径 | 说明 |
|-----------|------|
| `GET /status` | 角色(leader/follower)、leader、term、uptime、版本、配置来源 + **管线状态**（`pipeline_state`: idle/stock/running/paused/stopped、`pipeline_error`（paused 首因）、`pipeline_since`、`pipeline_paused_by_op`（paused 源于人工暂停而非致命错误，前端切换「恢复」入口）、`event_lag_seconds`（仅 running）——管线致命/拉取侧位点非法熔断后进程仍存活，此字段是「Web 看似正常但同步已停」的可视出口） |
| `GET /overview` | 摘要卡：事件 lag、吞吐(1m)、位点、队列深度、暂停规则数、stock 进度 |
| `GET /rules` | 规则列表 + 每规则状态（running/paused+原因）、各 sink 启用与最近错误 |
| `GET /rules/effective` | **每张表合并后生效的完整规则视图**（库级+表级合并的最终结果，库级规则展示为 `*` 通配行；数据源 = 规则编译输出。用途：用户不用脑跑继承合并算法；路由实现注意与 `/rules/{key}` 的字面量冲突，effective 取字面量优先） |
| `GET /rules/{key}/metrics` | 单表吞吐/lag 摘要 |
| `GET /position` | 当前位点（etcd 实时）、watermark、last_saved_seq |
| `GET /stock` | 全量任务列表：进度、断点、失败原因 |
| `GET /nodes` | 集群节点（etcd nodes 前缀 + 选举信息） |
| `GET /ddl-events` | 最近 DDL 事件与处置记录（etcd events/ddl） |
| `GET /config` | 当前生效配置（密码打码）+ etcd 中的待生效配置 diff |

### 操作（写，需二次确认参数 `confirm=true`；鉴权=全局 Basic 单凭据）

| 方法/路径 | 说明 |
|-----------|------|
| `POST /rules/{key}/pause` \| `/resume` | 暂停/恢复规则（sink 熔断/DDL pause 的解除） |
| `POST /rules/by-schema/{schema}/{table}/restock` | 清全量标记并重跑单表全量（confirm=true；分段参数支持含点号库名） |
| `POST /rules/{key}/restock` | 简写形态：`key` 按最后一个点拆分，表名不含点 |
| `POST /position` | 修改位点（danger，body 需带 `confirm:"SET-POSITION"` 原样字符串） |
| `POST /position/migrate-gtid` | **位点模式迁移**（P3）：file:pos 存量位点 → GTID 基线（use_gtid 切换后管线暂停的恢复路径）。confirm=true + 审计；前置 use_gtid=true 且位点缺 gtid_set；SHOW BINARY LOG STATUS 原子取三元组，要求同文件且差距 ≤ 8 MiB，否则 400 带指引 |
| `POST /position/migrate-filepos` | 反向迁移：GTID → file:pos（位点置最老 binlog 头，接受重放，下游幂等吸收）；confirm=true + 审计 |
| `POST /ddl/{event_id}/approve` \| `/skip` | DDL 审批（08 §5） |
| `POST /verify` | 抽样对账：body `{database, table, pk?, sample?}`，返回 `{src_count, tgt_count, checked, mismatch, diffs[]}`；数据差异是正常结果（code=0 看 mismatch），500 仅基础设施故障。对账唯一入口（无 CLI 子命令——Docker 部署下运维操作一律走 Web，无需进节点） |
| `PUT /config` | 写回 etcd 配置（提示需重启生效；不做热更） |
| `POST /leader/resign` | 主动让位（滚动发布用） |
| `POST /pipeline/pause` | **人工暂停管线**：写 etcd 持久标志 `{prefix}/pipeline-pause`（无 lease，跨重启/失位保持）+ 取消管线 ctx 走优雅停机（排空 + 位点终值落盘）。管线停止但进程/Leader/Web 存活（DDL 审批 gate 继续服务）；当选时发现标志存在则本任期不启动管线。用途：运维窗口（改位点/排查）不用停进程。非 leader 返回错误 |
| `POST /pipeline/resume` | **恢复人工暂停的管线**：删持久标志 + 等旧流程退出后从 etcd 位点重走全量检查/增量启动（支持暂停期间改位点）。非 leader / 非暂停态返回错误 |

### 响应与错误

统一 `{code, message, data}`；写操作产生审计事件（who=Basic 用户名、what、when、结果）写入日志与 etcd `events/audit`——**单 key 环形保留 100 条**（单 key CAS 并发安全；历史散 key 格式的数据在首次写入时一次性合并清理）。

etcd 治理（P2，leader 侧 `gcLoop` 每小时）：① stock 死标记（白名单外表的标记）删除，done 保留（「重启不重跑」依赖）；② 已决 DDL 记录 > 7 天删除，pending 永不删。

## 4. 页面清单（React SPA）

当前前端先消费已经落地的 API；页面不模拟缺失的指标、节点和全量任务数据。

| 页面 | 当前可实现内容 | 后端 API 状态 |
|------|------|------|
| Dashboard | 角色、leader、term、版本、配置来源、uptime、位点摘要；复杂趋势引导 Grafana | `/status`、`/position` 已有；`/overview` 尚未实现 |
| Pipelines / 规则 | `/rules/effective` 生效视图；已支持的单表 restock 与抽样 verify | 生效规则/restock/verify 已有；运行状态、暂停/恢复、每规则 metrics 尚未实现 |
| DDL Events | pending DDL、SQL、审批/跳过及语义提示 | `/ddl-events`、approve、skip 已有 |
| Position | 当前位点与危险修改表单 | GET/POST `/position` 已有 |
| Config | 打码后生效配置与「每表生效规则」只读标签页 | `/config`、`/rules/effective` 已有；etcd 配置编辑/diff 尚未实现 |
| Audit | 最近审计记录 | `/audit` 已有 |
| Full Sync / Nodes | 当前版本不展示伪造状态或无效操作 | `/stock`、`/nodes` 尚未实现 |

后端实现相应 API 后，再逐步启用 Full Sync、Nodes、运行态、配置写回等功能。

## 5. 安全

- `observability.web.username` + `password`（HTTP Basic 单凭据，全权限——一套凭据足够，危险操作另有 confirm 门禁与审计）；未配置凭据时拒绝启动（内网裸奔场景必须显式 `auth_disabled: true` 才放开，配置校验在公网部署告警）。Basic over 明文 HTTP 仅限内网；公网部署前置 TLS 反代。
- 无 Cookie/会话实现面；静态 SPA shell 与静态资源可匿名读取以呈现登录界面，但不包含敏感信息；所有 `/api/v1` 接口仍由 HTTP Basic 鉴权保护。前端凭据仅存当前页面内存。
- 写接口默认仅本机/内网：支持 `web.allow_cidrs` 白名单。
- 所有写操作幂等（重复提交安全），前端按钮二次确认。

## 6. 实现注意

- API 读取的是进程内实时状态（metrics registry、pipeline 状态机），**不提供**绕过管线的旁路写（除 position/etcd 配置两个受控入口）。
- `GET /rules/effective` 的数据源是规则编译产物（组形态白名单在 `internal/pipeline` 编译期直构的每表生效规则，**含全部白名单表**），**不是**把原始配置在前端重算一遍——前端只做展示分组。runtime 脱敏规则对外仅返回算法类型与必要参数，不返回解析后的 salt 或固定替换值。
- `order_by_column` 仅表示快照扫描排序列，不保证是唯一键；verify 前端不得将其自动当作 PK，须让操作者确认真实唯一索引列。
- 前端构建链：`cd web && npm ci && npm run build` → 产物输出 `internal/api/dist` → `go build`；`make build` 负责前端构建后再构建 Go。Docker 使用 Node 构建阶段生成静态资源，最终仍只部署 Go 单二进制。
- 本地开发：`npm --prefix web run dev`，默认将 `/api` 代理到 `http://127.0.0.1:8060`；可用 `VITE_API_PROXY_TARGET` 覆盖。检查命令：`npm --prefix web run typecheck`、`npm --prefix web run lint`、`npm --prefix web test`。
- 新仓库不含任何旧模板资产（React 端为全新实现）。

## 7. 多源扩展（源作用域 API 与前端）

2026-11 多源落地后的增量（完整设计见 `docs/prd/06`）：

- **源是一等路由段**：源私有资源挂 `/api/v1/sources/{sid}/...`（status/
  position/rules/ddl/pipeline/verify/migrate）；cluster 级（status 摘要、
  /sources 清单、/ddl-events 全集群合并、audit、config、leader）保持原位。
- **兼容层（弃用窗口 ≥ 2 个小版本）**：单源（default）部署旧路由等价别名、
  顶层管线字段镜像唯一源；多源部署旧路由 400 指引新路由、顶层管线字段置空
  以 `sources[]` 为准。前端一律走新路由——旧路由纯服务外部脚本/监控探针。
- **DTO**：StatusData 增加 `sources: SourceStatus[]`（装配 phase + 管线状态 +
  lag + 位点摘要）；DDLRecord/AuditEntry 增加 `source` 字段（源级操作可追溯）。
- **前端**：源作用域是路由级状态（URL query `?source={sid}`，刷新/书签/分享
  保持作用域）；顶栏源选择器（单源部署隐藏）；总控台聚合视图（汇总条 + 源
  卡片栅格 + 各页按源切分）；react-query key 全部含 sid（缓存隔离）。
