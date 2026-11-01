package stock

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"gocdc/internal/logging"
	"gocdc/internal/model"
	"gocdc/internal/schema"
	"gocdc/internal/sink"
	"gocdc/internal/transform"
)

// Scanner 单表 PK 游标扫描（06 §4：主键游标分块，规避深分页 OFFSET）。
type Scanner struct {
	schema    *schema.Manager
	transform *transform.Transformer
	sn        sink.Sink
	chunkRows int
}

// NewScanner 构造（chunkRows ≤0 = 5000）。
func NewScanner(sm *schema.Manager, tr *transform.Transformer, sn sink.Sink, chunkRows int) *Scanner {
	if chunkRows <= 0 {
		chunkRows = 5000
	}
	return &Scanner{schema: sm, transform: tr, sn: sn, chunkRows: chunkRows}
}

// Progress 单表扫描进度回调（标记持久化 + 指标）。
type Progress func(mk *Marker) error

// maxChunkRedials 单表扫描内连接重换上限（公网链路流龄强杀可能反复发生，
// 大表长扫需多次重连；10 次封顶防死循环烧连接）。
const maxChunkRedials = 10

// ScanTable 扫描一张表（slot = 会话槽位号，sql.Conn 不允许跨 goroutine 并发，
// 一槽一连接；断点续扫由 lastPK 决定起点；返回累计行数）。
// 块级连接丢失恢复：fetchChunk 遇连接类错误 → RedialSlot 换新连接 → 从当前
// PK 游标重试同一块（游标未推进，重读幂等；已投递块不受影响）——公网源库
// 长流被网关按流龄强杀场景下大表不再整表失败。
// 新连接为当前视图非 P 视图，一致性由 upsert 幂等 + ≥P 重放收敛保证
// （见 Session.RedialSlot 注释）。失败上抛；不可重试 → 表标记 failed，
// 绝不静默丢块——06 §4。
func (s *Scanner) ScanTable(ctx context.Context, sess *Session, slot int, db, table string,
	rule *model.Rule, lastPK []string, onProgress Progress) (int64, error) {

	meta, err := s.schema.Get(ctx, db, table)
	if err != nil {
		return 0, fmt.Errorf("stock: 加载 %s.%s 结构: %w", db, table, err)
	}
	pkCols := meta.PKColumns
	if len(pkCols) == 0 {
		return 0, fmt.Errorf("stock: %s.%s 无主键（全量要求主键排序键，05 §5.1.3）", db, table)
	}
	pkMeta := make([]model.Column, 0, len(pkCols))
	for _, name := range pkCols {
		col, _ := meta.ColumnByName(name)
		if col == nil {
			return 0, fmt.Errorf("stock: %s.%s 主键列 %s 不在结构中", db, table, name)
		}
		pkMeta = append(pkMeta, *col)
	}

	var (
		cursor   []any
		rowsDone int64
		chunkSeq int64
	)
	if len(lastPK) > 0 {
		if cursor, err = decodePK(pkMeta, lastPK); err != nil {
			return 0, err
		}
	}

	var redials int
	for {
		if err := ctx.Err(); err != nil {
			return rowsDone, err
		}
		rows, err := s.fetchChunk(ctx, sess.Conn(slot), db, table, meta, pkCols, cursor)
		if err != nil {
			if redials < maxChunkRedials && sink.IsConnLost(err) {
				redials++
				s.logRedial(db, table, redials, err)
				time.Sleep(time.Duration(redials) * 100 * time.Millisecond) // 轻退避
				if rerr := sess.RedialSlot(ctx, slot); rerr != nil {
					return rowsDone, fmt.Errorf("stock: %s.%s 第 %d 块重连失败: %w", db, table, chunkSeq, rerr)
				}
				continue // 游标未变，重读同一块
			}
			return rowsDone, err
		}
		if len(rows) == 0 {
			return rowsDone, nil // 空表/扫完
		}
		// 块间 schema 版本检测（06 §7：全量期间被 DDL → 该表 restock）
		if changed, err := s.schemaChanged(ctx, db, table, meta); err != nil {
			return rowsDone, err
		} else if changed {
			return rowsDone, fmt.Errorf("stock: %s.%s 全量期间发生 DDL，需整表 restock（06 §7）", db, table)
		}

		events := make([]*model.Event, 0, len(rows))
		for i, row := range rows {
			ev, err := s.transform.Row(transform.RowInput{
				Meta: meta, Rule: rule,
				Type:  model.EventInsert,
				Phase: model.PhaseSnapshot,
				EventID: fmt.Sprintf("%s.%s.snap.%s:%d.%d.%d",
					db, table, sess.Pos.Name, sess.Pos.Pos, chunkSeq, i),
				SourcePos: sess.Pos,
				After:     row, // 全量块 = 全列 INSERT 像（同一 transform/sink 路径，06 §6）
			})
			if err != nil {
				return rowsDone, fmt.Errorf("stock: %s.%s 行转换: %w", db, table, err)
			}
			events = append(events, ev)
		}
		// 块投递（同一 sink 路径；失败上抛不丢块）
		if err := s.sn.Send(ctx, events); err != nil {
			return rowsDone, fmt.Errorf("stock: %s.%s 第 %d 块投递: %w", db, table, chunkSeq, err)
		}

		// 游标推进 = 本块最后一批行的 PK 值（文本协议形态直接 encode）
		lastRow := rows[len(rows)-1]
		tuple := make([]any, 0, len(pkMeta))
		for i, idx := range pkIndexList(meta, pkCols) {
			tuple = append(tuple, normalizeForBind(pkMeta[i], lastRow[idx]))
		}
		cursor = tuple
		rowsDone += int64(len(rows))
		chunkSeq++

		if onProgress != nil {
			mk := &Marker{
				Status:    StatusRunning,
				SessionID: sess.ID,
				SnapshotP: sess.Pos,
				LastPK:    encodePK(tuple),
				ChunkSeq:  chunkSeq,
				RowsDone:  rowsDone,
			}
			if err := onProgress(mk); err != nil {
				return rowsDone, err
			}
		}
		if len(rows) < s.chunkRows {
			return rowsDone, nil // 末块
		}
	}
}

// logRedial 连接重换日志（Scanner 无 logger 字段，经包级 logging.L 兜底）。
func (s *Scanner) logRedial(db, table string, nth int, cause error) {
	logging.L().Warn("stock 连接丢失，换新连接续扫",
		zap.String("db", db), zap.String("table", table), zap.Int("nth", nth), zap.Error(cause))
}

// pkIndexList 主键列下标（与 pkCols 顺序一致）。
func pkIndexList(meta *model.TableMeta, pkCols []string) []int {
	idxs := make([]int, 0, len(pkCols))
	for _, name := range pkCols {
		_, idx := meta.ColumnByName(name)
		idxs = append(idxs, idx)
	}
	return idxs
}

// normalizeForBind 文本协议回读值 → 游标绑定值（按列类型；无符号回绕重解释同 04 §7）。
func normalizeForBind(col model.Column, v any) any {
	if v == nil {
		return nil
	}
	switch col.Type {
	case model.TypeTinyint, model.TypeSmallint, model.TypeMediumint, model.TypeInt, model.TypeBigint, model.TypeYear:
		var n int64
		switch x := v.(type) {
		case int64:
			n = x
		case []byte:
			if col.Unsigned {
				var u uint64
				_, _ = fmt.Sscanf(string(x), "%d", &u)
				return u
			}
			_, _ = fmt.Sscanf(string(x), "%d", &n)
		case string:
			if col.Unsigned {
				var u uint64
				_, _ = fmt.Sscanf(x, "%d", &u)
				return u
			}
			_, _ = fmt.Sscanf(x, "%d", &n)
		}
		if col.Unsigned {
			return uint64(n)
		}
		return n
	case model.TypeDecimal, model.TypeFloat, model.TypeDouble:
		if b, ok := v.([]byte); ok {
			return string(b)
		}
		return v
	default:
		if b, ok := v.([]byte); ok {
			return string(b)
		}
		return v
	}
}

// fetchChunk 拉取一块（conn = 本表快照连接；PK 行值游标 + ORDER BY 主键，06 §4）。
func (s *Scanner) fetchChunk(ctx context.Context, conn *sql.Conn, db, table string,
	meta *model.TableMeta, pkCols []string, cursor []any) ([][]any, error) {

	colList := make([]string, 0, len(meta.Columns))
	for i := range meta.Columns {
		colList = append(colList, meta.Columns[i].Name)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT %s FROM %s", quoteJoin(colList), quoteTable(db, table))
	if cursor != nil {
		fmt.Fprintf(&b, " WHERE (%s) > (%s)", quoteJoin(pkCols), placeholdersN(len(cursor)))
	}
	fmt.Fprintf(&b, " ORDER BY %s LIMIT %d", quoteJoin(pkCols), s.chunkRows)

	var args []any
	if cursor != nil {
		args = cursor
	}
	rows, err := conn.QueryContext(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("stock: %s.%s 扫描块: %w", db, table, err)
	}
	defer rows.Close()

	out := [][]any{}
	dest := make([]any, len(meta.Columns))
	ptrs := make([]any, len(meta.Columns))
	for i := range dest {
		ptrs[i] = &dest[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		cp := make([]any, len(dest))
		copy(cp, dest)
		out = append(out, cp)
	}
	return out, rows.Err()
}

// schemaChanged 块间结构变化检测（列名/类型指纹比对，06 §7）。
func (s *Scanner) schemaChanged(ctx context.Context, db, table string, meta *model.TableMeta) (bool, error) {
	fresh, err := s.schema.Refresh(ctx, db, table)
	if err != nil {
		return false, err
	}
	if len(fresh.Columns) != len(meta.Columns) {
		return true, nil
	}
	for i := range fresh.Columns {
		if fresh.Columns[i].Name != meta.Columns[i].Name ||
			fresh.Columns[i].RawType != meta.Columns[i].RawType {
			return true, nil
		}
	}
	return false, nil
}

func quoteTable(db, table string) string {
	return "`" + db + "`.`" + table + "`"
}

func quoteJoin(cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = "`" + strings.ReplaceAll(c, "`", "``") + "`"
	}
	return strings.Join(parts, ",")
}

func placeholdersN(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat("?,", n-1) + "?"
}
