// etcd 键空间 v1→v2 一次性迁移（多源 PRD 04 §4）：
// 升级后的进程首次启动装配期执行「legacy key → sources/default/」的搬家
//（配置已先行规范化为 v1→sources[default]，迁移目标源 id 恒为 default）。
//
// 算法（幂等 + 并发安全）：对每个键执行
//
//	Txn: If   新 key 不存在（Version == 0）
//	     Then Put(新, 旧值)（普通持久键——原键就是持久键，lease 保护在写门槛而非键本身）
//	          Delete(旧)
//	     Else  Delete(旧)   （幂等收尾：上次迁移写到一半重跑）
//
// 两节点同时启动执行同一批事务时，etcd 线性一致保证每个键恰有一个事务的
// If 成立——天然互斥，无需额外锁。全部键处理完后写完成标记
// {prefix}/migration/sources/v1tov2。任何一步 etcd 错误：重试由 etcdx 吸收，
// 仍失败则告警并放行启动——新代码只读新键，旧键残留无副作用；完成标记只在
// 清单全部处理完时写入。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"gocdc/internal/api"
	"gocdc/internal/config"
	"gocdc/internal/etcdx"
	"gocdc/internal/logging"
)

// migrateMarkerKey 完成标记 key（{prefix}/migration/sources/v1tov2）。
const migrateMarkerKey = "/migration/sources/v1tov2"

// migrateMarker 完成标记值。
type migrateMarker struct {
	Time  time.Time `json:"time"`
	Node  string    `json:"node_id"`
	Moved int       `json:"moved_keys"`
}

// migrateV1Keys 执行键迁移（main 装配期调用：setupCoordination 之后、源装配
// 之前）。返回搬迁键数与错误（错误仅日志告警——迁移失败不阻塞启动）。
func migrateV1Keys(ctx context.Context, kv *etcdx.KV, prefix, nodeID string) (int, error) {
	audit := api.NewAuditStore(kv, prefix, nodeID)

	// 完成标记已存在 → 全量搬完（或空环境首启已标记），跳过
	mctx, mcancel := context.WithTimeout(ctx, 10*time.Second)
	defer mcancel()
	if resp, err := kv.Get(mctx, prefix+migrateMarkerKey); err != nil {
		return 0, fmt.Errorf("读取迁移标记: %w", err)
	} else if len(resp.Kvs) > 0 {
		return 0, nil
	}

	// 搬迁清单（全部在 {prefix} 下，PRD 04 §4.1；leader/election/nodes/config/
	// events/audit 不迁）
	type pair struct{ oldKey, newKey string }
	var list []pair
	add := func(oldKey string) {
		list = append(list, pair{oldKey: prefix + oldKey, newKey: prefix + "/sources/" + config.DefaultSourceID + oldKey})
	}
	lctx, lcancel := context.WithTimeout(ctx, 15*time.Second)
	defer lcancel()
	singles := []string{"/position", "/pipeline-pause"}
	for _, s := range singles {
		if resp, err := kv.Get(lctx, prefix+s); err != nil {
			return 0, fmt.Errorf("扫描 %s: %w", s, err)
		} else if len(resp.Kvs) > 0 {
			add(s)
		}
	}
	for _, sub := range []string{"/stock/", "/events/ddl/"} {
		resp, err := kv.Get(lctx, prefix+sub, clientv3.WithPrefix())
		if err != nil {
			return 0, fmt.Errorf("扫描 %s: %w", sub, err)
		}
		for _, kvv := range resp.Kvs {
			rel := string(kvv.Key)[len(prefix):] // /stock/x 或 /events/ddl/y
			list = append(list, pair{oldKey: string(kvv.Key), newKey: prefix + "/sources/" + config.DefaultSourceID + rel})
		}
	}
	if len(list) == 0 {
		writeMigrateMarker(ctx, kv, prefix, nodeID, 0) // 空环境首启：直接标记完成
		return 0, nil
	}

	_ = audit.Append("system", "config.migrate_sources_v1tov2", fmt.Sprintf("start keys=%d", len(list)))
	logging.L().Info("etcd 键迁移 v1→v2 开始（存量键 → sources/default/）",
		zap.Int("keys", len(list)))

	moved := 0
	for _, p := range list {
		if err := migrateOneKey(ctx, kv, p.oldKey, p.newKey); err != nil {
			// 告警放行：旧键残留无副作用，下次启动重跑幂等收尾
			logging.L().Warn("etcd 键迁移单项失败（下次启动重试）",
				zap.String("old", p.oldKey), zap.Error(err))
			return moved, err
		}
		moved++
	}
	writeMigrateMarker(ctx, kv, prefix, nodeID, moved)
	logging.L().Info("etcd 键迁移完成",
		logging.Event(logging.EventEtcdKeysMigrated),
		zap.Int("moved", moved))
	_ = audit.Append("system", "config.migrate_sources_v1tov2", fmt.Sprintf("done moved=%d", moved))
	return moved, nil
}

// migrateOneKey 单键 CAS 搬家（幂等 + 并发安全，见文件头算法）。
func migrateOneKey(ctx context.Context, kv *etcdx.KV, oldKey, newKey string) error {
	octx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := kv.Get(octx, oldKey)
	if err != nil {
		return fmt.Errorf("读旧键: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return nil // 已搬走/不存在：跳过
	}
	val := string(resp.Kvs[0].Value)

	tctx, tcancel := context.WithTimeout(ctx, 10*time.Second)
	defer tcancel()
	_, terr := kv.Txn(tctx, func(t clientv3.Txn) clientv3.Txn {
		return t.If(clientv3.Compare(clientv3.Version(newKey), "=", 0)).
			Then(clientv3.OpPut(newKey, val), clientv3.OpDelete(oldKey)).
			Else(clientv3.OpDelete(oldKey)) // 新键已有（上次中断重跑）：幂等收尾
	})
	if terr != nil {
		return fmt.Errorf("事务: %w", terr)
	}
	return nil
}

// writeMigrateMarker 写完成标记（值含时间/节点/搬迁数，RUNBOOK 回滚对照用）。
func writeMigrateMarker(ctx context.Context, kv *etcdx.KV, prefix, nodeID string, moved int) {
	buf, err := json.Marshal(migrateMarker{Time: time.Now(), Node: nodeID, Moved: moved})
	if err != nil {
		return
	}
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := kv.Put(wctx, prefix+migrateMarkerKey, string(buf)); err != nil {
		logging.L().Warn("迁移完成标记写入失败（不影响运行，下次启动幂等重扫）", zap.Error(err))
	}
}
