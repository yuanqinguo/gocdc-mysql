import json, copy, sys

base = json.load(open("/mnt/d/workers/worker/qingfly/gocdc-mysql/.verify/config-node1.json", encoding="utf-8"))

# 源容器 system_time_zone=UTC（启动检查器告警项）：显式对齐，避免 TIMESTAMP 8h 漂移
base["source"]["timezone"] = "UTC"
# GTID 模式（源 gtid_mode=ON；主从切换免人工修位点）
base["source"]["use_gtid"] = True

# targets.rocketmq：v5 gRPC proxy endpoint（WSL 宿主 → 容器发布端口）
base["targets"]["rocketmq"] = {
    "enabled": True,
    "name_servers": ["127.0.0.1:8081"],
    "group": "gocdc-verify",
    "topic_prefix": "CDC_",
    "order_by": "row",
    "send_timeout_ms": 5000,
}
# rmqtest 规则：MySQL + RocketMQ 双写；全量 + 增量（幂等：先移除旧条目）
base["rules"] = [r for r in base["rules"] if r.get("schema") != "rmqtest"]
base["rules"].append({
    "schema": "rmqtest",
    "do_stock": True,
    "ddl_policy": "auto",
    "order_by_column": "id",
    "sinks": {"mysql": True, "rocketmq": True},
})
# cluster_e2e：集群回归用（纯 MySQL sink；默认 pause 策略）
base["rules"] = [r for r in base["rules"] if r.get("schema") != "cluster_e2e"]
base["rules"].append({
    "schema": "cluster_e2e",
    "do_stock": True,
    "order_by_column": "id",
    "sinks": {"mysql": True},
})

n1 = copy.deepcopy(base)
n2 = copy.deepcopy(base)
n2["node"]["id"] = "node2"
n2["source"]["server_id"] = 99002
n2["observability"]["metrics_addr"] = ":9596"
n2["observability"]["web"]["addr"] = ":8061"

for path, cfg in [("/mnt/d/workers/worker/qingfly/gocdc-mysql/.verify/config-node1.json", n1),
                  ("/mnt/d/workers/worker/qingfly/gocdc-mysql/.verify/config-node2.json", n2)]:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
print("configs written; rules:", [r["schema"] for r in n1["rules"]])
