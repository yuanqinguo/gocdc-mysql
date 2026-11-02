package sink

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gocdc/internal/model"
)

// Message 消息构建产物（05 §2.1）：Key 用于 Kafka 分区 / RocketMQ 顺序组，
// Value 为事件 JSON。
type Message struct {
	Key   string
	Value []byte
}

// order_by 语义（05 §2.3）：row = 主键粒度有序（推荐）；table = 整表全有序。
const (
	OrderByRow   = "row"
	OrderByTable = "table"
)

// BuildMessage 把事件编码为 MQ 消息（05 §2.1 消息格式；MQ sink 共用）。
//
// 消息自描述可追溯：event_id+position 可去重可追溯、before/after 明确命名、
// columns 自描述、phase 区分全量增量。sourceID 非空时写入 envelope source
// 字段（多源契约 v2，PRD D7：消费端按 (topic, event_id) 或 (source, event_id)
// 去重均成立——D6 保证 topic 不跨源相交）。
func BuildMessage(ev *model.Event, orderBy, sourceID string) (*Message, error) {
	envelope := map[string]any{
		"event_id": ev.EventID,
		"type":     string(ev.Type),
		"phase":    ev.Phase,
		"database": ev.Database,
		"table":    ev.Table,
		"ts":       ev.Ts,
		"exec_ts":  time.Unix(int64(ev.Ts), 0).Format(time.RFC3339),
		"position": map[string]any{
			"file": ev.SourcePos.Name,
			"pos":  ev.SourcePos.Pos,
		},
		"gtid":           ev.SourcePos.GTIDSet,
		"xid":            ev.XID,
		"pk":             encodeMap(ev.Pk),
		"schema_version": ev.SchemaVersion,
		"columns":        ev.Columns, // ColumnMeta 带 json tag，直接序列化
	}
	if sourceID != "" {
		envelope["source"] = sourceID
	}
	if ev.SQL != "" {
		envelope["sql"] = ev.SQL
	}
	if len(ev.Before) > 0 {
		envelope["before"] = encodeMap(ev.Before)
	} else {
		envelope["before"] = nil
	}
	if len(ev.After) > 0 {
		envelope["after"] = encodeMap(ev.After)
	} else {
		envelope["after"] = nil
	}

	value, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("sink: 事件 %s 序列化失败: %w", ev.EventID, err)
	}
	return &Message{Key: messageKey(ev, orderBy), Value: value}, nil
}

// messageKey 分区/顺序键（05 §2.3）：
//
//	row   → {db}.{table}.{pk值按列名字典序拼接}（同一行严格有序）
//	table → {db}.{table}（整表有序）
func messageKey(ev *model.Event, orderBy string) string {
	base := ev.Database + "." + ev.Table
	if orderBy != OrderByTable && len(ev.Pk) > 0 {
		cols := make([]string, 0, len(ev.Pk))
		for c := range ev.Pk {
			cols = append(cols, c)
		}
		sort.Strings(cols)
		key := base
		for _, c := range cols {
			key += "." + pkValueString(ev.Pk[c])
		}
		return key
	}
	return base
}

func pkValueString(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case string:
		return x
	case []byte:
		return string(x)
	case bool:
		return strconv.FormatBool(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", x)
	}
}

// encodeMap 列值走 04 §7 的消息编码规则（嵌套 map 逐值处理）。
func encodeMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = EncodeValue(v)
	}
	return out
}

// EncodeValue 单值消息编码（04 §7 补充）：
//   - json.RawMessage：原样嵌入（嵌套 JSON 值，04 §7 JSON 行）
//   - []byte：合法 UTF-8 → 字符串；否则 base64 + {"encoding":"base64"} 标记
//   - 其余（nil/bool/int64/uint64/float64/string）JSON 原生
func EncodeValue(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case model.RawMessage: // = json.RawMessage 别名；原样嵌入（内容已过 json.Valid 校验）
		return x
	case []byte:
		if utf8.Valid(x) {
			return string(x)
		}
		return map[string]any{
			"encoding": "base64",
			"data":     base64.StdEncoding.EncodeToString(x),
		}
	default:
		return v
	}
}

// TopicName 计算 MQ topic（05 §2.3），Kafka 与 RocketMQ 统一同一套命名：
// 规则覆盖优先；默认 topic_prefix + "{db}_{table}"，库名自身含点（如
// edu.mall.admin，多源库按 . 聚合命名）一并替换为下划线。点号全面不收：
// RocketMQ topic 字符集 ^[%|a-zA-Z0-9_-]+$ 点号非法（带点 broker 报
// ILLEGAL_TOPIC）；Kafka 虽允许点，但官方建议 topic 名不要混用 "." 与
// "_"（metrics 聚合按 replaceAll('.','_') 折叠，两种写法会同名串味），
// 故两个下游统一只用下划线（前缀字符集见配置校验）。
func TopicName(prefix, ruleOverride, dbName, tableName string) string {
	if ruleOverride != "" {
		return ruleOverride
	}
	return prefix + strings.ReplaceAll(dbName, ".", "_") + "_" + tableName
}

// OversizeError 消息超限预检失败（05 §7 poison：默认 PAUSED + 告警）。
type OversizeError struct {
	Topic string
	Size  int
	Limit int
}

func (e *OversizeError) Error() string {
	return fmt.Sprintf("sink: 消息 %d 字节超过 max_message_bytes %d（topic %s）", e.Size, e.Limit, e.Topic)
}

// Fingerprint 配置指纹（诊断用；如脱敏盐是否生效的间接核对）。
func Fingerprint(parts ...string) string {
	h := sha256.Sum256([]byte(fmt.Sprint(parts)))
	return hex.EncodeToString(h[:8])
}
