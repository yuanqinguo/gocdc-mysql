import io

entry = """
- 2026-10-29（晚）**全环境真实验证（WSL 容器 etcd/mysql:8.0.46 GTID/RocketMQ 5.3.1 proxy + 原生二进制，单点+集群两形态）抓到 8 P0+3 P1 并全部修复**（详见 docs/VERIFICATION-2026-10-29.md，Windows build/vet/test + WSL -race 15 包全绿）：① P0 target_database 同名回退缺失——sinks.go targetDB 闭包与 verify_tables.go 缺「空=同名」兜底，默认配置启动核对全表误报不存在拒启（E2E 集成测试显式设了 target_database 所以没暴露）；② P0 stock 单连接并发扫表——sql.Conn 不允许跨 goroutine，4 worker 共用 sess.conn 全表 driver: bad connection（集成测试仅 1 张待扫表未暴露）→ Session 改快照连接集：FTWRL 锁窗口内克隆 N 连接（mydumper 手法）一表一根、backup 模式单连接、bootstrap srcDB 池 4+workers；③ P0 elector 在位复核无超时——etcd 分区 leader 40s+ 不失位管线持续投递 → 复核加有界超时（2×周期且≤TTL/2）；④ P0 onLost 先阻塞注册更新再停管线——etcd 断连停管线被拖 12s+ → 先 cancel 再 2s 有界注册更新；⑤ P0 IsRetryable 不识别 gRPC 状态错误——RMQ broker 瞬断管线 fatal → status.FromError 分类；⑥ P0 不识别 ErrInvalidConn——目标库闪断管线 fatal → 哨兵+文本兜底；⑦ P0 GTID rotate 锚点丢 GTIDSet——use_gtid 下 rotate 后位点集合清空重启拒启 → source 初始化跟踪+Rotate 透传+dispatcher 锚点带集合；⑧ P0 DDL approve 竞态 fatal——审批挂起期间源表被删/改名则 approve 复验必败且重启复现 → ddlgen ErrSourceTableGone 哨兵 + worker 过期放行；⑨ P1 RMQ 默认 topic 含点 ILLEGAL_TOPIC → RocketMQTopicName({db}_{table})；⑩ P1 classify 漏 TOPIC_NOT_FOUND/ILLEGAL_TOPIC/类型不匹配永久分类；⑪ P1 RMQ Start 失败附 mqadmin 预建指引。运维语义确证（验证报告 §四）：位点回拨必须停机后做（优雅停机水位终值覆盖手工值）；RMQ topic 预建+FIFO 属性；白名单外 DDL approve=复验（人工先改目标）、白名单内 approve 一键全自动（含 CREATE 新表接入）；RENAME exclude 表新名不再命中排除；verify 不感知 mask/复合主键需显式 pk。新增回归测试：TestRocketMQTopicName/TestClassify 扩展/TestIsRetryable/sink errors_test。遗留：MaskSpec json tag（P3）、启动核对对新表过严（pause+CREATE 可审批自动建表但核对要求目标预建）、Kafka 容器级验证、T7.4 专项 crash 注入。
"""

with io.open(r"D:\workers\worker\qingfly\gocdc-mysql\docs\PROGRESS.md", encoding="utf-8") as f:
    lines = f.readlines()

lines.insert(114, entry)

with io.open(r"D:\workers\worker\qingfly\gocdc-mysql\docs\PROGRESS.md", "w", encoding="utf-8", newline="\n") as f:
    f.writelines(lines)
print("inserted")
