#!/usr/bin/env python3
"""RMQ 消费数据 vs MySQL 源/目标 对账。

输入：rmqcons 产出的 JSONL（每行一条事件信封）
逻辑：按到达序重放事件（RocketMQ 同 message group 保序），得到每 PK 终态；
      与源库/目标库全表逐行逐列比对（id,name,qty,amount,note）。
"""
import json
import subprocess
import sys

JSONL = sys.argv[1] if len(sys.argv) > 1 else "/tmp/rmq-msgs.jsonl"
TABLE = "rmqtest.msg"
COLS = ["id", "name", "qty", "amount", "note"]


def dump(container, password, db):
    q = "SELECT " + ",".join(COLS) + f" FROM {TABLE} ORDER BY id"
    out = subprocess.run(
        ["docker", "exec", container, "mysql", "-uroot", f"-p{password}", "-h127.0.0.1",
         "--default-character-set=utf8mb4", "-N", "-e", q],
        capture_output=True, text=True, check=True).stdout
    rows = {}
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) != len(COLS):
            continue
        vals = dict(zip(COLS, parts))
        rows[int(vals["id"])] = vals
    return rows


def norm_msg(row):
    """消息 after → 与 MySQL 文本形态对齐（NULL 用 <NULL> 标记）。"""
    out = {}
    for c in COLS:
        v = row.get(c)
        if c == "id" or c == "qty":
            out[c] = "" if v is None else str(int(v))
        elif v is None:
            out[c] = "<NULL>"
        else:
            out[c] = str(v)
    return out


def main():
    events = []
    with open(JSONL, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))

    stats = {}
    seen = {}
    dup = 0
    state = {}
    for ev in events:
        key = (ev["type"], ev["phase"])
        stats[key] = stats.get(key, 0) + 1
        eid = ev["event_id"]
        if eid in seen:
            dup += 1
            continue  # 至少一次投递去重
        seen[eid] = True
        if ev["type"] == "DDL":
            continue  # DDL 消息无行像（结构事件），不参与终态重放
        pk = int(ev["pk"]["id"])
        if ev["type"] == "DELETE":
            state.pop(pk, None)
        else:
            state[pk] = norm_msg(ev["after"])

    src = dump("vf-src", "123456", "rmqtest")
    dst = dump("vf-dst", "123456", "rmqtest")

    print("events:", len(events), "dup_event_id:", dup)
    print("by type/phase:", dict(sorted(stats.items())))
    print(f"rmq_final_rows={len(state)} src_rows={len(src)} dst_rows={len(dst)}")

    diffs = 0
    for name, ref in (("SRC", src), ("DST", dst)):
        missing = set(ref) - set(state)
        extra = set(state) - set(ref)
        if missing:
            print(f"{name}: missing_in_rmq={sorted(missing)[:10]} (n={len(missing)})")
            diffs += len(missing)
        if extra:
            print(f"{name}: extra_in_rmq={sorted(extra)[:10]} (n={len(extra)})")
            diffs += len(extra)
        for pk in sorted(set(ref) & set(state)):
            if ref[pk] != state[pk]:
                diffs += 1
                if diffs <= 5:
                    print(f"{name}: DIFF pk={pk} mysql={ref[pk]} rmq={state[pk]}")
    print("RESULT:", "PASS" if diffs == 0 else f"FAIL diffs={diffs}")
    sys.exit(0 if diffs == 0 else 1)


if __name__ == "__main__":
    main()
