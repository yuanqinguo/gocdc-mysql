# examples · 三形态示例配置

| 文件 | 形态 | 要点 |
|------|------|------|
| `single.yml` | 单节点 | `ha.enabled: false`——无选举，进程直接 leader；位点仍走 etcd 租约门槛 |
| `cluster.yml` | HA 集群 | `ha.enabled: true`——同配置多副本，etcd 选举唯一 leader，standby 热备接管 |
| `multi-source.yml` | 多源 | `sources[]`——单进程双源（src-a/src-b）聚合写入同一目标库，V4 校验保证目标不相交 |

三份均为 `config_version: 2` 完整字段（不留默认省略），字段全集见
`internal/config/README.md`；校验规则见 `docs/prd/03-config-schema.md`。

## 与部署形态的对应

- 示例地址为本项目验证环境映射端口（`127.0.0.1:13306` 源 / `13307` 目标 /
  多源第二实例 `13406`）；docker-compose 部署把地址换成服务名
  （如 `mysql-src:3306`，参考 `deploy/docker-compose*.yml`）。
- 密码仅演示取明文，生产一律 `${ENV}` 注入（`03 §3.2`）——web 密码与
  web/metrics 监听地址均已用 `${WEB_PASSWORD}`/`${WEB_ADDR}`/`${METRICS_ADDR}`
  占位（集群多副本共用一份配置，各节点注入不同端口避免冲突）。
- 两种启动形态（`arch/03 §3.1`）：零参数 + 工作目录 `config.local.yml`
  引导（`config_source: etcd`），或纯 flag `-cluster <n> -etcd <addrs>`。

## 验收

`.verify/v2/examples_accept.sh`（WSL 验证环境）把三份配置逐个写入配置
中心、独立集群前缀 boot、断言数据面：

1. `single.yml` → `role=single`，全量快照 + 增量落目标；
2. `cluster.yml` → 双节点 `leader/follower`，增量 + resign 切换续传；
3. `multi-source.yml` → 两源 `running`，各自增量落 `ms_merged`。

复跑：`wsl -d Ubuntu-22.04 -- bash /mnt/d/course/gocdc-mysql/.verify/v2/examples_accept.sh`
（前台执行，依赖 `.verify/gocdc` 二进制已构建）。
