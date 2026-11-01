package stock

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"gocdc/internal/model"
)

// CheckPurged 增量启动前的 binlog 保留检查（06 §7）：P 所在文件已被清理
// → 拒绝启动增量（报错指明需 restock），绝不带缺口静默运行。
func CheckPurged(ctx context.Context, db *sql.DB, p model.Position) error {
	if p.Name == "" {
		return nil // 无位点（引导路径）不适用
	}
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		// 无 REPLICATION CLIENT 权限等情况：无法判断，放行并交给同步报错
		return nil
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	var files []string
	for rows.Next() {
		// 列数随版本变化（5.7 两列；8.0+: File/Size/Encrypted）：动态 Scan，只取首列文件名
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		if name, ok := vals[0].([]byte); ok {
			files = append(files, string(name))
		} else if s, ok := vals[0].(string); ok {
			files = append(files, s)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}
	sort.Strings(files)
	if p.Name < files[0] { // 位点文件早于现存最老 binlog → 已被清理
		return fmt.Errorf("stock: 位点 %s 早于现存最老 binlog %s（binlog 已被清理，缺口不可补）——"+
			"请执行 restock 重跑全量后再启动增量（06 §7）", p.Name, files[0])
	}
	return nil
}
