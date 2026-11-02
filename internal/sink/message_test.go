package sink

import (
	"encoding/json"
	"testing"

	"gocdc/internal/model"
)

func e2eEvent() *model.Event {
	return &model.Event{
		EventID:  "bin.000001:100:0",
		Type:     model.EventUpdate,
		Phase:    model.PhaseBinlog,
		Database: "db1", Table: "users",
		Ts: 1761624000, XID: 99,
		SourcePos: model.Position{Name: "bin.000001", Pos: 200, GTIDSet: "gtid-1"},
		Pk:        map[string]any{"id": int64(7)},
		Before:    map[string]any{"id": int64(7), "name": "old", "blob": []byte{0xFF}},
		After: map[string]any{
			"id": int64(7), "name": "new",
			"amount": "12.34",
			"ext":    model.RawMessage(`{"k":[1]}`),
			"blob":   []byte("héllo"),
			"bin":    []byte{0xFF, 0xFE}, // 非法 UTF-8
		},
		Columns: map[string]model.ColumnMeta{
			"id":     {Type: "bigint"},
			"amount": {Type: "decimal", Precision: 10, Scale: 2, AsString: true},
			"name":   {Type: "varchar", Masked: "fixed"},
			"ext":    {Type: "json"},
		},
		SchemaVersion: 3,
	}
}

func TestBuildMessage_Golden(t *testing.T) {
	m, err := BuildMessage(e2eEvent(), OrderByRow, "")
	if err != nil {
		t.Fatal(err)
	}
	var top map[string]any
	if err := json.Unmarshal(m.Value, &top); err != nil {
		t.Fatal(err)
	}
	if top["event_id"] != "bin.000001:100:0" || top["type"] != "UPDATE" ||
		top["phase"] != "binlog" || top["database"] != "db1" || top["table"] != "users" {
		t.Fatalf("头字段: %v", top)
	}
	if top["exec_ts"] == "" || top["xid"] != float64(99) || top["schema_version"] != float64(3) {
		t.Fatalf("时间/事务/版本: %v", top)
	}
	pos := top["position"].(map[string]any)
	if pos["file"] != "bin.000001" || pos["pos"] != float64(200) {
		t.Fatalf("position: %v", pos)
	}
	if top["gtid"] != "gtid-1" {
		t.Fatalf("gtid: %v", top["gtid"])
	}
	// pk / before / after
	pk := top["pk"].(map[string]any)
	if pk["id"] != float64(7) {
		t.Fatalf("pk: %v", pk)
	}
	before := top["before"].(map[string]any)
	if before["name"] != "old" {
		t.Fatalf("before.name: %v", before["name"])
	}
	// 非 UTF-8 字节 → base64 + 标记（04 §7）
	bb := before["blob"].(map[string]any)
	if bb["encoding"] != "base64" || bb["data"] != "/w==" {
		t.Fatalf("before.blob: %v", bb)
	}
	after := top["after"].(map[string]any)
	if after["name"] != "new" || after["amount"] != "12.34" {
		t.Fatalf("after 标量: %v", after)
	}
	// JSON 列嵌套原样（不转义成字符串）
	ext, ok := after["ext"].(map[string]any)
	if !ok || ext["k"].([]any)[0].(float64) != 1 {
		t.Fatalf("after.ext 应为嵌套 JSON: %#v", after["ext"])
	}
	// 合法 UTF-8 字节 → 字符串
	if after["blob"] != "héllo" {
		t.Fatalf("after.blob: %v", after["blob"])
	}
	// 非法 UTF-8 → base64
	bin := after["bin"].(map[string]any)
	if bin["encoding"] != "base64" {
		t.Fatalf("after.bin: %v", bin)
	}
	// columns 自描述
	cols := top["columns"].(map[string]any)
	amount := cols["amount"].(map[string]any)
	if amount["type"] != "decimal" || amount["as_string"] != true ||
		amount["precision"] != float64(10) || amount["scale"] != float64(2) {
		t.Fatalf("columns.amount: %v", amount)
	}
	if cols["name"].(map[string]any)["masked"] != "fixed" {
		t.Fatalf("columns.name.masked: %v", cols["name"])
	}
}

func TestBuildMessage_Keys(t *testing.T) {
	ev := e2eEvent()
	if k, _ := BuildMessage(ev, OrderByRow, ""); k.Key != "db1.users.7" {
		t.Fatalf("row key: %s", k.Key)
	}
	// 联合主键按列名字典序拼接
	ev.Pk = map[string]any{"tenant": "a", "id": int64(7)}
	if k, _ := BuildMessage(ev, OrderByRow, ""); k.Key != "db1.users.7.a" {
		t.Fatalf("复合主键 key: %s", k.Key)
	}
	if k, _ := BuildMessage(ev, OrderByTable, ""); k.Key != "db1.users" {
		t.Fatalf("table key: %s", k.Key)
	}
}

// Topic 默认命名 Kafka/RocketMQ 统一（sink.TopicName）：前缀 + "{db}_{table}"，
// 点号全面不收——RocketMQ topic 字符集 ^[%|a-zA-Z0-9_-]+$ 点号非法（带点报
// ILLEGAL_TOPIC）；Kafka 官方建议不混用 "." 与 "_"（metrics 聚合同名串味）。
func TestTopicName(t *testing.T) {
	if got := TopicName("cdc_", "", "db1", "users"); got != "cdc_db1_users" {
		t.Fatalf("topic: %s", got)
	}
	if got := TopicName("cdc_", "custom.topic", "db1", "users"); got != "custom.topic" {
		t.Fatalf("override topic: %s", got)
	}
	// 带点库名（edu.mall.admin，多源库 . 聚合命名）——库名内点也须替换，
	// 否则默认名含点会被 RocketMQ broker 拒绝（producer 启动报 ILLEGAL_TOPIC）
	if got := TopicName("CDC_", "", "edu.mall.admin", "users"); got != "CDC_edu_mall_admin_users" {
		t.Fatalf("dotted db topic: %s", got)
	}
}
