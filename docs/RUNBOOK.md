# gocdc 运维手册（Runbook）

> 完整 SLA/拓扑/参数速查见 docs/arch/；多源设计见 docs/prd/。本文只写故障处置
> 动作。**全部运维操作经 admin Web（`/api/v1`，HTTP Basic 单用户名密码 +
> confirm）**——不提供 CLI 子命令（Docker 部署下不应要求进节点执行命令；
> `-h` 只剩服务模式 flag）。个别无 Web 界面的操作（如手工播种位点）用
> etcdctl 直接写对应 key。
>
> **多源部署（配置 v2 `sources[]`）**：源级操作一律走 `/api/v1/sources/{sid}/...`
>（位点回拨/restock/DDL 审批/暂停恢复/verify 全部按源）；旧顶层路由已移除
>（`/position`、`/pipeline/*` 等一律 404），单源部署同样走 `/sources/default/...`。
> etcd 直写键全部下沉一层：
> `/gocdc/{cluster}/sources/{sid}/position|stock/*|pipeline-pause|events/ddl/*`。
> 单源 v1 部署升级后 sid 恒为 `default`。

## 0. 日常检查
- Grafana `gocdc-overview`：leader 状态、事件延迟、位点滞后、暂停规则、DLQ 计数。
- 告警通道：Critical（无 leader / 管线暂停 / 位点保存失败 / DLQ）必报到人。

## 1. 位点回拨（目标端污染需重放）
1. Grafana/DBA 确认污染窗口起点（binlog file+pos）。
2. **先停进程**（运行中回拨的位点会被优雅停机的水位终值覆盖）。
3. Web `POST /api/v1/position`（confirm=SET-POSITION）回拨位点到窗口起点
   （进程已停时也可 etcdctl 直接写 `{prefix}/position`）。
4. 重启进程 → 从新位点重放 → MySQL upsert / MQ 消费端按 event_id 去重消化重复。

## 2. 强制重跑全量（restock）
- Web `POST /api/v1/rules/{db.table}/restock?confirm=true` → 清标记。
- 重启进程 → onElected 检测无标记自动重跑全量（快照对 + 增量衔接）。

## 3. 新增表接入
1. 改配置（rules 加库级/表级规则）：Web「规则页 → 编辑规则」（校验+活体预检+CAS
   写配置中心，`PUT /api/v1/sources/{sid}/rules`）或 etcdctl 直改配置中心键
   （默认 `/config/edu.mall.gocdc/system`；多集群共享 etcd 时为显式区分键，如
   `/gocdc/{cluster}/config`）。
2. 重启（配置不热更；Web 保存成功后可直接「应用并重启」=
   `POST /api/v1/config/apply-restart?confirm=true`，优雅排空+位点终值+resign
   后退出，由编排拉起）。新表无 stock 标记 → 自动全量 → 增量接管。

## 4. 源库主从切换
- GTID 模式：自动续接（推荐生产开启）。
- file:pos 模式：停实例 → 从新主 SHOW BINARY LOG STATUS 取位点 →
  Web `POST /api/v1/position`（confirm=SET-POSITION）写入 → 启动。
  切换间隙丢失不可恢复——强烈建议 GTID。

## 5. DDL 挂起处理（ddl_policy=pause / 白名单外）
1. 告警 `gocdc_pipeline_paused{reason="ddl"}=1`；Web `GET /api/v1/ddl-events` 查看 SQL。
2. 评估：安全 → `POST /api/v1/ddl/{id}/approve?confirm=true`（执行目标 DDL+resume）；
   危险 → 人工改目标表后 approve（复验通过即 resume）；跳过 → skip（按新 schema 继续，审计留痕）。

## 6. etcd 故障
- 单节点宕机：无影响。quorum 丢失：lease 全过期 → 全节点停管线（安全态）→
  恢复后自动重选（≤ TTL+10s）。写进 SLA 说明。

## 7. 下游迁移（换 Kafka/目标库）
sink 配置变更 + 暂停 → 切换 → 恢复。位点不受影响。

### RocketMQ 下游前置
- topic 须**预先在 broker 创建**（v5 客户端只拉路由不建 topic）：
  `mqadmin updateTopic -n <namesrv> -b <broker:10911> -t <topic> -a +message.type=FIFO`
  （顺序消息必须 FIFO 属性，否则发送报 message type not match → 管线 PAUSED）。
- 默认 topic 名 = `{topic_prefix}{db}_{table}`（点号非法，不得复用 Kafka 命名）。

## 8. 发布前置检查
无暂停规则；lag < 阈值；旧实例已停（server_id 冲突互踢）。

## 9. 多源运维（配置 v2 sources[]）

### 9.1 多源配置变更 / 新增源
1. 改配置：Web 编辑（`PUT /api/v1/sources/{sid}/rules`，按源整组提交，服务端
   全量校验 + 活体预检 + ModRevision CAS 防并发覆盖）或 etcdctl 直改配置中心键
   （默认 `/config/edu.mall.gocdc/system`；多集群共享 etcd 时为显式区分键）
   （v2 YAML：`sources[]` 数组）→ 滚动重启生效（不热更，现行口径）。
2. **集群滚动重启顺序**：先逐个重启 follower（带新配置回来当热备），最后重启
   leader（`POST /api/v1/config/apply-restart?confirm=true` 或编排层重启）。
   顺序颠倒不产生数据风险（旧配置=少同步，安全方向），只浪费一轮重启。
3. 新增源 = 配置加一项 + 重启：新源首启自动全量，其余源照常（各源独立全量，
   不存在「等最慢源」屏障）。**Web 编辑只覆盖既有源的 rules 段**；加源/改
   source/target 连接仍走 etcdctl/配置文件（编辑端点对 file 形态显式 409）。
4. 跨源目标不相交在启动校验期拦死（MySQL 三元组/Kafka topic/RocketMQ topic
   冲突即拒启）——两个源写同一个 topic/目标表属配置错误。
5. 同一源实例被两个源订阅：server_id 必须显式互异（校验强制），否则互踢。

### 9.2 单源故障处置
- 定位：`/status` sources[] 或 `gocdc_source_up{source}=0` / `gocdc_sources{state="failed"}`。
- 处置：进入该源单源视图按 §1~§7 流程操作，路由带 `/sources/{sid}`；
  其余源不受影响（失败隔离，D5）。
- 某源源库主从切换：GTID 模式自动续传；file:pos 停该源（其他源不停——
  整进程重启仍会全部源同时进入切换窗口，需绝对隔离见多 cluster 形态）。

### 9.3 etcd 键迁移回滚（v2 降级到旧版本二进制）
旧版本代码只认旧键；迁移后降级会「丢」位点（旧键已删）。回滚三步：
1. 停进程；
2. `etcdctl get /gocdc/{cluster}/sources/default/position --print-value-only`
   确认现行位点；
3. `etcdctl put /gocdc/{cluster}/position <上述值>`（手工搬回；stock 与 DDL
   门同理，DDL 门通常量少可只搬 pending 记录）→ 换回旧二进制启动。
位点值结构（model/position.go）与快照标记结构跨版本不变，搬回即可用。

### 9.4 升级检查单（v1 → 多源就绪）
1. 配置正文先改写：顶层 `source/targets/rules` 包一层
   `sources: [{id: "default", ...}]`（id 必须填 `default`——键迁移固定落
   sources/default/，改名即对不上位点）。顶层旧形态已被新二进制拒绝
   （2026-11 移除自动包装）。
2. 新二进制启动一次：日志出现 `etcd_keys_migrated`（键迁移完成，旧
   position/stock/DDL 键搬到 sources/default/）。
3. 指标出现 `source="default"` label（源私有指标 label 化；告警查询同步
   更新——不带 source 的选择器仍命中，sum 类聚合不受影响）。
4. Web 顶栏出现源选择器（多源时；单源隐藏）。

### 9.5 容量规划（部署 checklist）
- 源数上限 64（配置校验硬上限）。
- 目标 MySQL 连接 = Σ 各源 max_open_conns（指向同一实例时全部落同一实例，
  目标库 max_connections 按合计规划）。
- 源库连接 = 每源 `4 + stock.workers`（打向各自源库）。
- GOMEMLIMIT 建议 `N × 150MB + 100MB` 起步；单源内存上界 ≈ 现行单集群，
  进程上界 ≈ N × 单源 + 共享面（数十 MB）。
- leader 切换/重启：全部源同时进入切换窗口（每源中断 ≈ 单次切换窗口，
  同时发生）——需错峰的场景保留多 cluster 形态共存。
