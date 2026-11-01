// 源库/位点辅助：DSN 构造、binlog 尾部位点采样、位点先后比较。
package main

import (
	"context"
	"database/sql"
	"fmt"

	"gocdc/internal/config"
	"gocdc/internal/model"
)

// laterBinlog 位点先后比较（文件名字典序 + 偏移；全量交接 max(P) 推导用）。
func laterBinlog(a, b model.Position) bool {
	if a.Name != b.Name {
		return a.Name > b.Name
	}
	return a.Pos > b.Pos
}

// sourceDSN 源库 DSN（binlog 拉取账号需要 REPLICATION SLAVE/CLIENT 权限）。
// 注意：go-sql-driver v1.10.1 不支持 tcpKeepAlive DSN 参数（未知参数会被当作
// 服务端系统变量转发 → Error 1193），keepalive 为驱动默认开启（OS 默认间隔）。
// 公网源库长流被网关强杀的恢复靠 stock 块级重连（Scanner.fetchChunk 重试路径）。
func sourceDSN(s config.Source) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/?charset=%s", s.User, s.Password, s.Addr, s.Charset)
}

// showMasterStatus 当前 binlog 尾部位点（首启引导 / lag 口径采样）。
// MySQL 8.2+ 语句更名 SHOW BINARY LOG STATUS，旧版回退 SHOW MASTER STATUS。
// 返回 Executed_Gtid_Set（GTID 口径 lag 判定依赖）。
func showMasterStatus(ctx context.Context, db *sql.DB) (model.Position, error) {
	var name, g string
	var pos uint32
	var i1, i2 any
	if err := db.QueryRowContext(ctx, "SHOW BINARY LOG STATUS").Scan(&name, &pos, &i1, &i2, &g); err != nil {
		if e2 := db.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(&name, &pos, &i1, &i2, &g); e2 != nil {
			return model.Position{}, e2
		}
	}
	return model.Position{Name: name, Pos: pos, GTIDSet: g}, nil
}

// oldestBinlog 最老保留 binlog 文件名（GTID→file:pos 反向迁移的重放起点，
// Web migrate-filepos 用）。SHOW BINARY LOGS 列数 5.7 两列 / 8.0 三列
// （File/Size/Encrypted）——按 Columns() 动态 Scan（同 06 §7 PURGED 检查）。
func oldestBinlog(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return "", err
		}
		switch n := vals[0].(type) {
		case []byte:
			return string(n), nil
		case string:
			return n, nil
		}
	}
	return "", rows.Err()
}
