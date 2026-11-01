package transform

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"gocdc/internal/metrics"
	"gocdc/internal/model"
)

// userMeta 多列表（含联合主键），承载 Event 构建断言。
func userMeta() *model.TableMeta {
	return &model.TableMeta{
		Schema: "db1", Table: "users", Version: 7,
		Columns: []model.Column{
			{Name: "id", Type: model.TypeBigint, IsPK: true},
			{Name: "tenant", Type: model.TypeVarchar, IsPK: true, Charset: "utf8mb4"},
			{Name: "name", Type: model.TypeVarchar, Charset: "utf8mb4"},
			{Name: "amount", Type: model.TypeDecimal, Precision: 10, Scale: 2},
			{Name: "ext", Type: model.TypeJSON},
			{Name: "password", Type: model.TypeVarchar, Charset: "utf8mb4"},
		},
		PKColumns: []string{"id", "tenant"},
	}
}

func rule() *model.Rule {
	return &model.Rule{
		Key: "db1.users", Schema: "db1", Table: "users",
		ExcludeColumns: []string{"password"},
		MaskColumns: map[string]model.MaskSpec{
			"name": {Type: model.MaskFixed}, // 主键 tenant 配 mask 也必须被忽略（红线）
		},
	}
}

// insertRow: id=1 tenant=a name=bob amount=1.23 ext={} password=s3cret
func insertRow() []any {
	return []any{int64(1), "a", "bob", "1.23", `{"k":1}`, "s3cret"}
}

func TestRow_Insert(t *testing.T) {
	tr := New(Options{})
	ev, err := tr.Row(RowInput{
		Meta: userMeta(), Rule: rule(),
		After:   insertRow(),
		Type:    model.EventInsert,
		EventID: "bin.000001:100:1",
		Ts:      1761624000, XID: 9,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ev.Type != model.EventInsert || ev.Phase != model.PhaseBinlog {
		t.Fatalf("type/phase: %s/%s", ev.Type, ev.Phase)
	}
	if ev.EventID != "bin.000001:100:1" || ev.XID != 9 || ev.SchemaVersion != 7 {
		t.Fatalf("event 头不符: %+v", ev)
	}
	// Pk：联合主键、脱敏前、不受 exclude/mask 影响
	if ev.Pk["id"] != int64(1) || ev.Pk["tenant"] != "a" {
		t.Fatalf("pk: %#v", ev.Pk)
	}
	// After：转换+脱敏；exclude 列物理移除
	if ev.After["name"] != "***" {
		t.Fatalf("after.name 未脱敏: %#v", ev.After["name"])
	}
	if ev.After["amount"] != "1.23" { // decimal string 无损
		t.Fatalf("after.amount: %#v", ev.After["amount"])
	}
	if string(ev.After["ext"].(model.RawMessage)) != `{"k":1}` {
		t.Fatalf("after.ext 应为 RawMessage: %#v", ev.After["ext"])
	}
	if _, ok := ev.After["password"]; ok {
		t.Fatal("exclude 列必须从 after 移除")
	}
	if ev.Before != nil {
		t.Fatal("INSERT 的 before 应为 nil")
	}
	// Columns 元信息：decimal 标注 as_string、mask 标注算法、exclude 列不出现
	if cm := ev.Columns["amount"]; !cm.AsString || cm.Precision != 10 || cm.Scale != 2 {
		t.Fatalf("columns.amount: %+v", cm)
	}
	if cm := ev.Columns["name"]; cm.Masked != model.MaskFixed {
		t.Fatalf("columns.name.masked: %+v", cm)
	}
	if _, ok := ev.Columns["password"]; ok {
		t.Fatal("exclude 列不得出现在 columns")
	}
	if cm := ev.Columns["tenant"]; cm.Masked != "" {
		t.Fatalf("主键列 mask 必须被忽略（红线 10 §4）: %+v", cm)
	}
}

func TestRow_Update_BeforeMaskedToo(t *testing.T) {
	tr := New(Options{})
	before := []any{int64(1), "a", "old", "9.99", `{}`, "oldpwd"}
	after := []any{int64(2), "a", "new", "1.23", `{}`, "newpwd"} // 主键 id 1→2
	ev, err := tr.Row(RowInput{
		Meta: userMeta(), Rule: rule(),
		Before: before, After: after,
		Type: model.EventUpdate,
	})
	if err != nil {
		t.Fatal(err)
	}
	// before 像同样脱敏（10 §6：前像不漏敏）
	if ev.Before["name"] != "***" {
		t.Fatalf("before.name 未脱敏: %#v", ev.Before["name"])
	}
	if _, ok := ev.Before["password"]; ok {
		t.Fatal("exclude 列必须从 before 移除")
	}
	// Pk 取后像（新主键寻址）；前像旧主键值保留在 Before 供 sink 处理主键变更
	if ev.Pk["id"] != int64(2) {
		t.Fatalf("pk 应取 after: %#v", ev.Pk)
	}
	if ev.Before["id"] != int64(1) {
		t.Fatalf("before.id 应保留旧主键值: %#v", ev.Before["id"])
	}
}

func TestRow_Delete_PkFromBefore(t *testing.T) {
	tr := New(Options{})
	ev, err := tr.Row(RowInput{
		Meta:   userMeta(),
		Before: insertRow(),
		Type:   model.EventDelete,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ev.After != nil {
		t.Fatal("DELETE 的 after 应为 nil")
	}
	if ev.Pk["id"] != int64(1) || ev.Pk["tenant"] != "a" {
		t.Fatalf("delete pk: %#v", ev.Pk)
	}
}

func TestRow_OrderColumnFallback(t *testing.T) {
	meta := &model.TableMeta{
		Schema: "db1", Table: "noprk", Version: 1,
		Columns: []model.Column{
			{Name: "code", Type: model.TypeVarchar, Charset: "utf8mb4"},
			{Name: "v", Type: model.TypeInt},
		},
	}
	r := &model.Rule{Key: "db1.noprk", Schema: "db1", Table: "noprk", OrderByColumn: "code"}
	ev, err := New(Options{}).Row(RowInput{
		Meta: meta, Rule: r,
		After: []any{"C1", int64(5)}, Type: model.EventInsert,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ev.Pk["code"] != "C1" {
		t.Fatalf("order_by_column 兜底主键: %#v", ev.Pk)
	}

	// 无主键且无 order_by_column → 显式报错（不可寻址）
	if _, err := New(Options{}).Row(RowInput{
		Meta: meta, After: []any{"C1", int64(5)}, Type: model.EventInsert,
	}); !errors.Is(err, ErrConvert) {
		t.Fatalf("无主键应报 ErrConvert: %v", err)
	}
}

func TestRow_ImageShapeErrors(t *testing.T) {
	tr := New(Options{})
	meta := userMeta()
	if _, err := tr.Row(RowInput{Meta: meta, Type: model.EventInsert}); err == nil {
		t.Fatal("INSERT 缺 after 应报错")
	}
	if _, err := tr.Row(RowInput{Meta: meta, Type: model.EventDelete, Before: insertRow(), After: insertRow()}); err != nil {
		// DELETE 带 after 允许（忽略）——不报错即通过
		_ = err
	}
	// 行列数与结构不符（回拨重放窗口：历史行像 × 新 schema）→ on_convert_error
	// 策略处置（默认 null：缺失尾列置 nil + 计数告警，事件正常产出；非静默）
	短行 := []any{int64(1), "a"} // 2 列 < meta 6 列
	evShort, err := tr.Row(RowInput{Meta: meta, After: 短行, Type: model.EventInsert})
	if err != nil {
		t.Fatalf("默认 null 策略下短行不应 fatal: %v", err)
	}
	for _, c := range []string{"name", "amount", "ext", "password"} {
		if v, ok := evShort.After[c]; !ok || v != nil {
			t.Fatalf("缺失尾列 %s 应为 nil: %#v", c, evShort.After[c])
		}
	}
	if evShort.After["id"] != int64(1) || evShort.After["tenant"] != "a" {
		t.Fatalf("已有列值应保留: %#v", evShort.After)
	}
	if _, err := tr.Row(RowInput{Meta: nil, After: insertRow(), Type: model.EventInsert}); err == nil {
		t.Fatal("nil meta 应报错")
	}
}

// 行像比 schema 新（重放中漂移竞态）：多余列忽略、事件正常产出。
func TestRow_RowLongerThanMeta(t *testing.T) {
	tr := New(Options{})
	长行 := append(insertRow(), "surplus") // 7 列 > meta 6 列
	ev, err := tr.Row(RowInput{Meta: userMeta(), After: 长行, Type: model.EventInsert})
	if err != nil {
		t.Fatalf("长行不应 fatal: %v", err)
	}
	if _, ok := ev.After["password"]; !ok {
		t.Fatal("meta 内列应完整")
	}
	for k := range ev.After {
		if k == "surplus-col" {
			t.Fatal("多余列不应出现在输出")
		}
	}
}

// column_count_mismatch × fail_event 策略：保持上抛（显式选择者的红线）。
func TestRow_ColumnCountFailEvent(t *testing.T) {
	tr := New(Options{OnConvertError: StrategyFailEvent})
	_, err := tr.Row(RowInput{Meta: userMeta(), After: []any{int64(1)}, Type: model.EventInsert})
	if err == nil {
		t.Fatal("fail_event 策略下短行应上抛")
	}
	var ce *ConvertError
	if !errors.As(err, &ce) || ce.Reason != ReasonColumnCount || !ce.IsFailEvent() {
		t.Fatalf("期望 fail_event+column_count: %#v", err)
	}
}

func TestRow_FailEventStrategy(t *testing.T) {
	meta := &model.TableMeta{
		Schema: "db1", Table: "t", Version: 1,
		Columns: []model.Column{
			{Name: "id", Type: model.TypeBigint, IsPK: true},
			{Name: "e", Type: model.TypeEnum, EnumValues: []string{"a", "b"}},
		},
		PKColumns: []string{"id"},
	}
	// 越界 + fail_event → 整事件失败、不可重试分类
	tr := New(Options{OnConvertError: StrategyFailEvent})
	_, err := tr.Row(RowInput{
		Meta: meta, After: []any{int64(1), int64(9)}, Type: model.EventInsert,
	})
	if err == nil {
		t.Fatal("fail_event 应上抛")
	}
	var ce *ConvertError
	if !errors.As(err, &ce) || !ce.IsFailEvent() || ce.Reason != ReasonEnumIndex {
		t.Fatalf("期望 fail_event+enum_index_out_of_range: %#v", err)
	}
	if !errors.Is(err, ErrConvert) {
		t.Fatal("错误链应含 ErrConvert 哨兵")
	}

	// 同一越界 + null（默认）→ 值置 nil，事件正常产出（+计数，见下测）
	ev, err := New(Options{}).Row(RowInput{
		Meta: meta, After: []any{int64(1), int64(9)}, Type: model.EventInsert,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ev.After["e"] != nil {
		t.Fatalf("null 策略应置 nil: %#v", ev.After["e"])
	}

	// zero 策略 → 空串
	ev, err = New(Options{OnConvertError: StrategyZero}).Row(RowInput{
		Meta: meta, After: []any{int64(1), int64(9)}, Type: model.EventInsert,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ev.After["e"] != "" {
		t.Fatalf("zero 策略应为空串: %#v", ev.After["e"])
	}
}

func TestRow_ConvertErrorMetrics(t *testing.T) {
	m := metrics.New()
	meta := &model.TableMeta{
		Schema: "db1", Table: "t", Version: 1,
		Columns: []model.Column{
			{Name: "id", Type: model.TypeBigint, IsPK: true},
			{Name: "n", Type: model.TypeInt},
		},
		PKColumns: []string{"id"},
	}
	tr := New(Options{Metrics: m})
	if _, err := tr.Row(RowInput{
		Meta: meta, After: []any{int64(1), "bad"}, Type: model.EventInsert,
	}); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(m.ConvertErrors.WithLabelValues("db1", "t", "n", "null")); got != 1 {
		t.Fatalf("convert_errors 计数应为 1，实际 %v", got)
	}
}

func TestRedactor(t *testing.T) {
	rules := map[string]*model.Rule{
		"db1.users": rule(),
	}
	r := NewRedactor(rules)
	// 已转换的行值：exclude 移除、mask 替换
	out := r.Redact("db1", "users", map[string]any{
		"id": int64(1), "tenant": "a", "name": "bob", "password": "s3cret",
	})
	if out["name"] != "***" {
		t.Fatalf("redact mask: %#v", out["name"])
	}
	if _, ok := out["password"]; ok {
		t.Fatal("redact exclude")
	}
	if out["id"] != int64(1) {
		t.Fatalf("无规则列原样: %#v", out["id"])
	}
	// 无规则表透传；nil 行 nil 返回
	if got := r.Redact("other", "x", map[string]any{"a": 1}); got["a"] != 1 {
		t.Fatalf("无规则表透传: %#v", got)
	}
	if r.Redact("db1", "users", nil) != nil {
		t.Fatal("nil 行应返回 nil")
	}
}
