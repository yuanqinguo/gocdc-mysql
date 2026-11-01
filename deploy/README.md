# gocdc 部署 / 演示

## 拓扑

| 服务 | 用途 | 端口（宿主机） |
|------|------|--------------|
| etcd | 配置/选举/位点 | 2379 |
| mysql-src | 源库（ROW binlog） | 13306 |
| mysql-dst | 目标库 | 13307 |
| gocdc-1 | 同步节点 1 | 9595 (metrics/healthz) |
| gocdc-2 | 同步节点 2（`--profile cluster` 时加入） | 9596 |

## 运行

```bash
cd deploy

# 单节点
docker compose up -d

# 集群演示（双节点选主：kill 掉 leader，观察 gocdc-2 在 lease TTL 内接管）
docker compose --profile cluster up -d

# 状态 / 日志
docker compose ps
docker compose logs -f gocdc-1
```

## 验证同步

```bash
# 1. 等全部 healthy：docker compose ps
# 2. metrics/健康端点
curl -s http://127.0.0.1:9595/healthz
curl -s http://127.0.0.1:9595/metrics | grep gocdc_
curl -s http://127.0.0.1:9596/metrics | grep gocdc_leader_state   # leader=1 的那个节点（gocdc-2 端口）
# 3. 写入源库
docker compose exec mysql-src mysql -uroot -p123456 -e \
  "INSERT INTO demo.orders (user_id, amount, remark, phone) VALUES (1, 9.90, 'hello', '13800001234');"
# 4. 查目标库
docker compose exec mysql-dst mysql -uroot -p123456 -e "SELECT * FROM demo.orders\G"
```

## 说明

- 演示用 `-config-file`（03 §3.1 开发模式）；位点/选举仍走 etcd——所以多节点选主是真 HA。
- 零参数启动：工作目录放 `config.local.yml` 即可（引导文件 file/etcd 两形态、
  配置中心与协调面可分实例，见 internal/config/README.md §2.3）；compose 演示用显式 flag 形态。
- 生产形态：配置写入 etcd `{prefix}/config` 后只给 `-cluster/-etcd` 启动（Web 端 M6 提供）。
- 示例配置里的密码仅用于演示，生产用 `${ENV}` 占位注入（03 §3.2）。
- gocdc 启动期会比对源/目标全部唯一索引；列序相同但索引名称不同是允许的，缺失或额外唯一键会 fail-fast（防止幂等 upsert 按错误唯一键折叠数据）。
- 镜像由 CI（tag_push）构建推送；本地 compose 默认现场 build（`build:` 段）。
- 镜像 tag 约定：tag `dev-v1.2.3` → `$REGISTRY/$SLUG/dev:v1.2.3`。
