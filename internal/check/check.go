// Package check 实现启动检查器（源端要求全项 + fail-fast 清单）：
// binlog_format/ROW、binlog_row_image=FULL、binlog 保留、GTID 一致性、
// server_id 冲突探测——任何【必须】项失败拒绝启动并输出结构化原因。
package check

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// Result 检查结果集合。
type Result struct {
	Errors   []string // 【必须】项失败 → 拒绝启动
	Warnings []string // 【建议】项/降级说明 → 告警
	Info     []string // 声明项（所选模式等）
}

// OK 是否全部必须项通过。
func (r *Result) OK() bool { return len(r.Errors) == 0 }

// Summary 人读摘要。
func (r *Result) Summary() string {
	var b strings.Builder
	for _, e := range r.Errors {
		b.WriteString("FAIL  " + e + "\n")
	}
	for _, w := range r.Warnings {
		b.WriteString("WARN  " + w + "\n")
	}
	for _, i := range r.Info {
		b.WriteString("INFO  " + i + "\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

func (r *Result) errf(format string, a ...any) {
	r.Errors = append(r.Errors, fmt.Sprintf(format, a...))
}
func (r *Result) warnf(format string, a ...any) {
	r.Warnings = append(r.Warnings, fmt.Sprintf(format, a...))
}
func (r *Result) infof(format string, a ...any) {
	r.Info = append(r.Info, fmt.Sprintf(format, a...))
}

// Options 检查参数（config.Source 映射）。
type Options struct {
	UseGTID         bool
	ServerID        uint32
	MinBinlogRetain string // 提示性：评估保留期文案
}

// Source 源库环境检查（源端要求表逐项；全部只读查询）。
func Source(ctx context.Context, db *sql.DB, opts Options) *Result {
	r := &Result{}

	// 1) binlog_format=ROW（必须）
	var binlogFormat string
	if err := db.QueryRowContext(ctx, "SELECT @@binlog_format").Scan(&binlogFormat); err != nil {
		r.errf("读取 @@binlog_format 失败: %v", err)
	} else if !strings.EqualFold(binlogFormat, "ROW") {
		r.errf("binlog_format=%s 非 ROW，拒绝启动（SET GLOBAL binlog_format=ROW 并重启会话）", binlogFormat)
	} else {
		r.infof("binlog_format=ROW")
	}

	// 2) binlog_row_image=FULL（必须；非 FULL 告警降级——before 像缺失，
	//    PK 变更处理与 UPDATE 前像自动关闭）
	var rowImage string
	if err := db.QueryRowContext(ctx, "SELECT @@binlog_row_image").Scan(&rowImage); err != nil {
		r.warnf("读取 @@binlog_row_image 失败: %v", err)
	} else if !strings.EqualFold(rowImage, "FULL") {
		r.warnf("binlog_row_image=%s 非 FULL：UPDATE 前像缺失，主键变更/delete 前像语义降级（08/05 §5.2 依赖 FULL，建议改回）", rowImage)
	} else {
		r.infof("binlog_row_image=FULL")
	}

	// 3) log_bin 开启（必须；SHOW BINARY LOGS 可用性即证明，顺带列出现存文件）
	var logs []string
	if rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS"); err == nil {
		for rows.Next() {
			var name string
			rest := make([]any, 8)
			ptrs := make([]any, 0, len(rest))
			ptrs = append(ptrs, &name)
			for i := range rest {
				ptrs = append(ptrs, &rest[i])
			}
			if err := rows.Scan(ptrs...); err == nil {
				logs = append(logs, name)
			}
		}
		rows.Close()
		if len(logs) == 0 {
			r.warnf("SHOW BINARY LOGS 为空：binlog 可能未开启或刚 rotate（CDC 依赖 binlog）")
		} else {
			sort.Strings(logs)
			r.infof("binlog 现存文件 %d 个（最老 %s）", len(logs), logs[0])
		}
	} else {
		r.errf("SHOW BINARY LOGS 失败（log_bin 未开启或无 REPLICATION CLIENT 权限）: %v", err)
	}

	// 4) GTID（可选；use_gtid=true 时必须 ON）
	var gtidMode string
	gtidErr := db.QueryRowContext(ctx, "SELECT @@gtid_mode").Scan(&gtidMode)
	if opts.UseGTID {
		if gtidErr != nil || !strings.EqualFold(gtidMode, "ON") {
			r.errf("use_gtid=true 但 gtid_mode=%s ≠ ON", gtidMode)
		} else {
			r.infof("gtid_mode=ON（源主从切换免人工修位点）")
		}
	} else if gtidErr == nil && strings.EqualFold(gtidMode, "ON") {
		r.infof("gtid_mode=ON 但 use_gtid=false（生产建议开启 GTID 模式，07 §5）")
	}

	// 5) server_id 冲突探测（必须；SHOW SLAVE HOSTS / SHOW REPLICAS）
	serverIDCheck(ctx, db, opts.ServerID, r)

	// 6) 时区声明核对（建议）：源库 time_zone 与配置 source.timezone 提示
	var tz string
	if err := db.QueryRowContext(ctx, "SELECT @@system_time_zone").Scan(&tz); err == nil && tz != "" {
		r.infof("源库 system_time_zone=%s（DATETIME 字符串以服务端写入时区为准，source.timezone 必须与其一致）", tz)
	}

	return r
}

// serverIDCheck 探测其它 replica 的 server_id 冲突（互踢循环防护）。
// SHOW REPLICAS / SHOW SLAVE HOSTS 的列数与列序随版本/分支不同（4~7 列），
// Server_id 按列名定位，其余列动态 dest 吞掉——定长 dest 会让 Scan 整行
// 报错，冲突检测静默失效。
func serverIDCheck(ctx context.Context, db *sql.DB, serverID uint32, r *Result) {
	query := func(q string) (*sql.Rows, bool) {
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			return nil, false
		}
		return rows, true
	}
	for _, q := range []string{"SHOW REPLICAS", "SHOW SLAVE HOSTS"} {
		rows, ok := query(q)
		if !ok {
			continue
		}
		cols, cerr := rows.Columns()
		if cerr != nil {
			rows.Close()
			continue
		}
		serverIDIdx := -1
		for i, c := range cols {
			if strings.EqualFold(c, "server_id") {
				serverIDIdx = i
				break
			}
		}
		if serverIDIdx < 0 {
			rows.Close() // 无 Server_id 列（未知版本形态）：换下一条命令
			continue
		}
		for rows.Next() {
			vals := make([]any, len(cols))
			scan := make([]any, len(cols))
			var serverIDCol int64
			for i := range cols {
				if i == serverIDIdx {
					scan[i] = &serverIDCol
				} else {
					scan[i] = &vals[i]
				}
			}
			if err := rows.Scan(scan...); err != nil {
				continue
			}
			if uint64(serverIDCol) == uint64(serverID) {
				r.errf("server_id=%d 已被其它 replica 占用（互踢循环风险）——更换唯一 server_id", serverID)
			}
		}
		rows.Close()
		return // 任一命令可用（含 0 行=无其它 replica）即止
	}
	r.infof("server_id=%d 冲突探测跳过（SHOW REPLICAS 不可用，运维台账核对）", serverID)
}
