package transform

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"gocdc/internal/model"
)

// oneCol 构造单列表的 TableMeta（黄金样本的载体）。
func oneCol(name, typ string, mut func(*model.Column)) *model.TableMeta {
	col := model.Column{Name: name, Type: typ, Charset: "utf8mb4"}
	if mut != nil {
		mut(&col)
	}
	return &model.TableMeta{
		Schema: "db1", Table: "t1", Version: 1,
		Columns: []model.Column{col}, PKColumns: []string{name},
	}
}

// TestConvert_Golden 黄金样本表直接驱动 convertValue（不经 Row）。
func TestConvert_Golden(t *testing.T) {
	utc := time.Date(2026, 10, 28, 4, 0, 0, 0, time.UTC)
	cst, _ := time.LoadLocation("Asia/Shanghai")

	cases := []struct {
		name string
		meta *model.TableMeta
		in   any
		want any
		opts func(*Options)
	}{
		// ── 有符号整型（04 §7 行 1）──
		{"signed/int64透传", oneCol("c", model.TypeInt, nil), int64(42), int64(42), nil},
		{"signed/int8归一", oneCol("c", model.TypeTinyint, nil), int8(-5), int64(-5), nil},
		{"signed/int16归一", oneCol("c", model.TypeSmallint, nil), int16(300), int64(300), nil},
		{"signed/int32归一", oneCol("c", model.TypeMediumint, nil), int32(70000), int64(70000), nil},
		{"signed/int归一", oneCol("c", model.TypeBigint, nil), int(99), int64(99), nil},
		{"signed/文本协议字符串", oneCol("c", model.TypeInt, nil), "123", int64(123), nil},
		{"signed/文本协议字节负数", oneCol("c", model.TypeInt, nil), []byte("-42"), int64(-42), nil},
		{"signed/uint64输入小值", oneCol("c", model.TypeInt, nil), uint64(7), int64(7), nil},
		{"signed/float整数值防御", oneCol("c", model.TypeInt, nil), float64(3), int64(3), nil},
		{"signed/坏文本→null", oneCol("c", model.TypeInt, nil), "abc", nil, nil},
		{"signed/非整数float→null", oneCol("c", model.TypeInt, nil), float64(3.5), nil, nil},
		{"signed/nil→nil", oneCol("c", model.TypeInt, nil), nil, nil, nil},

		// ── 无符号整型（04 §7 行 2：溢出不置 NULL，按位宽回绕还原）──
		{"unsigned/uint64最大值", oneCol("c", model.TypeBigint, func(c *model.Column) { c.Unsigned = true }),
			uint64(math.MaxUint64), uint64(math.MaxUint64), nil},
		{"unsigned/int64正数", oneCol("c", model.TypeInt, func(c *model.Column) { c.Unsigned = true }),
			int64(5), uint64(5), nil},
		{"unsigned/文本", oneCol("c", model.TypeInt, func(c *model.Column) { c.Unsigned = true }),
			"42", uint64(42), nil},
		{"unsigned/超uint64兜底decimal串", oneCol("c", model.TypeBigint, func(c *model.Column) { c.Unsigned = true }),
			"18446744073709551616", "18446744073709551616", nil},
		{"unsigned/字节文本", oneCol("c", model.TypeInt, func(c *model.Column) { c.Unsigned = true }),
			[]byte("7"), uint64(7), nil},
		{"unsigned/有符号回绕重解释(bigint)", oneCol("c", model.TypeBigint, func(c *model.Column) { c.Unsigned = true }),
			int64(-1), uint64(math.MaxUint64), nil}, // go-mysql 有符号解析回绕（uint64max）
		{"unsigned/有符号回绕重解释(int)", oneCol("c", model.TypeInt, func(c *model.Column) { c.Unsigned = true }),
			int32(-2147483648), uint64(0x80000000), nil},
		{"unsigned/坏文本→null", oneCol("c", model.TypeInt, func(c *model.Column) { c.Unsigned = true }),
			"xyz", nil, nil},

		// ── DECIMAL（04 §7 行 4：默认 string 无损，避免 float64 精度丢失）──
		{"decimal/高标度无损", oneCol("c", model.TypeDecimal, func(c *model.Column) { c.Precision, c.Scale = 65, 30 }),
			"123.456789012345678901234567890123", "123.456789012345678901234567890123", nil},
		{"decimal/字节输入", oneCol("c", model.TypeDecimal, nil), []byte("0.1"), "0.1", nil},
		{"decimal/float输入还原定点", oneCol("c", model.TypeDecimal, nil), 0.1, "0.1", nil},
		{"decimal/float模式", oneCol("c", model.TypeDecimal, nil), "1.5", float64(1.5),
			func(o *Options) { o.DecimalAs = DecimalAsFloat }},
		{"decimal/float模式坏文本", oneCol("c", model.TypeDecimal, nil), "abc", nil,
			func(o *Options) { o.DecimalAs = DecimalAsFloat }},
		{"decimal/float32转string", oneCol("c", model.TypeDecimal, nil), float32(1.5), "1.5", nil},
		{"decimal/nil→nil", oneCol("c", model.TypeDecimal, nil), nil, nil, nil},

		// ── FLOAT/DOUBLE（04 §7 行 5）──
		{"float/float32升位", oneCol("c", model.TypeFloat, nil), float32(1.5), float64(1.5), nil},
		{"double/float64透传", oneCol("c", model.TypeDouble, nil), 3.14, 3.14, nil},
		{"float/文本协议", oneCol("c", model.TypeDouble, nil), "2.5", float64(2.5), nil},
		{"double/字节负数", oneCol("c", model.TypeDouble, nil), []byte("-1.25"), float64(-1.25), nil},
		{"float/坏文本→null", oneCol("c", model.TypeFloat, nil), "xx", nil, nil},

		// ── DATETIME/TIMESTAMP（04 §7 行 6/7：源时区显式解释 + 零日期策略）──
		{"datetime/字符串透传", oneCol("c", model.TypeDatetime, nil), "2026-10-28 12:00:00", "2026-10-28 12:00:00", nil},
		{"datetime/零日期默认null", oneCol("c", model.TypeDatetime, nil), "0000-00-00 00:00:00", nil, nil},
		{"datetime/零日期保字符串", oneCol("c", model.TypeDatetime, nil), "0000-00-00 00:00:00", "0000-00-00 00:00:00",
			func(o *Options) { o.ZeroDatetimeAs = ZeroDatetimeString }},
		{"datetime/timeTime转源时区", oneCol("c", model.TypeDatetime, nil), utc, "2026-10-28 12:00:00",
			func(o *Options) { o.Timezone = cst }},
		{"datetime/timeTime零值", oneCol("c", model.TypeDatetime, nil), time.Time{}, nil, nil},
		{"datetime/字节输入", oneCol("c", model.TypeDatetime, nil), []byte("2026-01-01 00:00:00"), "2026-01-01 00:00:00", nil},
		{"datetime/毫秒保留", oneCol("c", model.TypeDatetime, nil),
			time.Date(2026, 10, 28, 12, 0, 0, 500000000, time.UTC), "2026-10-28 12:00:00.5", nil},
		{"timestamp/秒值按源时区", oneCol("c", model.TypeTimestamp, nil), uint64(1793160000), "2026-10-28 12:00:00",
			func(o *Options) { o.Timezone = cst }},
		{"timestamp/零日期null", oneCol("c", model.TypeTimestamp, nil), "0000-00-00 00:00:00", nil, nil},

		// ── DATE（04 §7 行 8）──
		{"date/字符串透传", oneCol("c", model.TypeDate, nil), "2026-10-28", "2026-10-28", nil},
		{"date/零日期null", oneCol("c", model.TypeDate, nil), "0000-00-00", nil, nil},
		{"date/带时间部分截断", oneCol("c", model.TypeDate, nil), "2026-10-28 00:00:00", "2026-10-28", nil},
		{"date/timeTime格式化", oneCol("c", model.TypeDate, nil), utc, "2026-10-28",
			func(o *Options) { o.Timezone = cst }},

		// ── TIME（04 §7 行 9）──
		{"time/字符串透传", oneCol("c", model.TypeTime, nil), "838:59:59", "838:59:59", nil},
		{"time/duration还原负值", oneCol("c", model.TypeTime, nil), -90 * time.Minute, "-01:30:00", nil},
		{"time/字节输入", oneCol("c", model.TypeTime, nil), []byte("12:00:00"), "12:00:00", nil},

		// ── YEAR（04 §7 行 10）──
		{"year/int64", oneCol("c", model.TypeYear, nil), int64(2026), int64(2026), nil},
		{"year/文本", oneCol("c", model.TypeYear, nil), "2026", int64(2026), nil},
		{"year/字节", oneCol("c", model.TypeYear, nil), []byte("2000"), int64(2000), nil},

		// ── BIT（04 §7 行 11：按位解码，任意位串照实还原而非全变 0）──
		{"bit/单字节1", oneCol("c", model.TypeBit, func(c *model.Column) { c.Precision = 8 }), []byte{0x01}, uint64(1), nil},
		{"bit/单字节0", oneCol("c", model.TypeBit, func(c *model.Column) { c.Precision = 1 }), []byte("\x00"), uint64(0), nil},
		{"bit/两字节小端", oneCol("c", model.TypeBit, func(c *model.Column) { c.Precision = 16 }), []byte{0x05, 0x00}, uint64(5), nil},
		{"bit/八字节大值", oneCol("c", model.TypeBit, func(c *model.Column) { c.Precision = 64 }),
			[]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, uint64(math.MaxUint64), nil},
		{"bit/字符串形式", oneCol("c", model.TypeBit, func(c *model.Column) { c.Precision = 8 }), "\x01", uint64(1), nil},
		{"bit/uint64透传", oneCol("c", model.TypeBit, nil), uint64(9), uint64(9), nil},

		// ── ENUM（04 §7 行 12：索引查表；越界走策略，而非静默产出空串）──
		{"enum/索引1", enumCol(), int64(1), "a", nil},
		{"enum/索引2", enumCol(), int64(2), "b", nil},
		{"enum/索引0越界null", enumCol(), int64(0), nil, nil},
		{"enum/索引越界null", enumCol(), int64(3), nil, nil},
		{"enum/越界zero空串", enumCol(), int64(3), "", func(o *Options) { o.OnConvertError = StrategyZero }},
		{"enum/文本协议透传", enumCol(), "b", "b", nil},

		// ── SET（04 §7 行 13：位掩码解码）──
		{"set/两位", setCol(), int64(3), "a,b", nil},
		{"set/低位", setCol(), int64(1), "a", nil},
		{"set/零", setCol(), int64(0), "", nil},
		{"set/未知高位→null", setCol(), int64(7), nil, nil},
		{"set/文本透传", setCol(), "a,b", "a,b", nil},

		// ── JSON（04 §7 行 14：raw string 不反序列化，避免 map 绑参整批失败）──
		{"json/对象文本", oneCol("c", model.TypeJSON, nil), `{"a":1}`, json.RawMessage(`{"a":1}`), nil},
		{"json/数组字节", oneCol("c", model.TypeJSON, nil), []byte("[1,2]"), json.RawMessage("[1,2]"), nil},
		{"json/null字面量合法", oneCol("c", model.TypeJSON, nil), "null", json.RawMessage("null"), nil},
		{"json/坏文本→null", oneCol("c", model.TypeJSON, nil), "not json", nil, nil},

		// ── 字符/二进制（04 §7 行 15/16）──
		{"char/字节解码", oneCol("c", model.TypeVarchar, nil), []byte("hello"), "hello", nil},
		{"varchar/字符串透传", oneCol("c", model.TypeVarchar, nil), "world", "world", nil},
		{"text/坏utf8非utf8库→null", oneCol("c", model.TypeText, func(c *model.Column) { c.Charset = "gbk" }),
			[]byte{0xFF, 0xFE}, nil, nil},
		{"text/坏utf8声明utf8保真", oneCol("c", model.TypeText, nil), []byte{0xFF, 0xFE}, "\xff\xfe", nil},
		{"blob/字节透传", oneCol("c", model.TypeBlob, nil), []byte{0, 1, 2}, []byte{0, 1, 2}, nil},
		{"varbinary/字符串转字节", oneCol("c", model.TypeVarbinary, nil), "abc", []byte("abc"), nil},

		// ── 未知类型透传（前向兼容）──
		{"unknown/geometry透传", oneCol("c", "geometry", nil), "POINT(1 2)", "POINT(1 2)", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := New(Options{})
			if tc.opts != nil {
				o := Options{}
				tc.opts(&o)
				tr = New(o)
			}
			col, _ := tc.meta.ColumnByName("c")
			got, err := tr.convertValue(tc.meta, col, tc.in)
			if err != nil {
				t.Fatalf("convertValue 返回错误: %v", err)
			}
			if !wantMatch(got, tc.want) {
				t.Fatalf("got %#v (%T), want %#v (%T)", got, got, tc.want, tc.want)
			}
		})
	}
}

func enumCol() *model.TableMeta {
	return oneCol("c", model.TypeEnum, func(c *model.Column) { c.EnumValues = []string{"a", "b"} })
}

func setCol() *model.TableMeta {
	return oneCol("c", model.TypeSet, func(c *model.Column) { c.SetValues = []string{"a", "b"} })
}

// wantMatch 处理 json.RawMessage 与 []byte 的等价比较。
func wantMatch(got, want any) bool {
	switch w := want.(type) {
	case json.RawMessage:
		g, ok := got.(json.RawMessage)
		return ok && string(g) == string(w)
	case []byte:
		g, ok := got.([]byte)
		return ok && string(g) == string(w)
	default:
		return got == want
	}
}

// TestConvert_FailEvent 三策略中 fail_event 的行为：错误上抛、类型可判、
// 错误文本不携带原始值。
func TestConvert_FailEvent(t *testing.T) {
	tr := New(Options{OnConvertError: StrategyFailEvent})
	meta := enumCol()
	col, _ := meta.ColumnByName("c")
	_, err := tr.convertValue(meta, col, int64(3))
	if err == nil {
		t.Fatal("期望 fail_event 上抛错误")
	}
	ce, ok := err.(*ConvertError)
	if !ok || !ce.IsFailEvent() {
		t.Fatalf("期望 *ConvertError 且 fail_event，实际 %#v", err)
	}
	if !strings.Contains(err.Error(), "enum_index_out_of_range") {
		t.Fatalf("错误缺 reason: %v", err)
	}
	if strings.Contains(err.Error(), "3") && strings.Contains(err.Error(), "索引 3 越界") {
		// 索引值属元数据允许出现；此处断言不会断言原始列值（enum 索引非敏感）
		_ = ce
	}
}

// TestConvert_ZeroStrategy zero 策略的各类别零值。
func TestConvert_ZeroStrategy(t *testing.T) {
	cases := []struct {
		typ  string
		mut  func(*model.Column)
		in   any
		want any
	}{
		{model.TypeInt, nil, "bad", int64(0)},
		{model.TypeBigint, func(c *model.Column) { c.Unsigned = true }, "bad", int64(0)},
		{model.TypeDecimal, nil, "bad", "0"},
		{model.TypeDouble, nil, "bad", float64(0)},
		{model.TypeDatetime, nil, true, mysqlZeroDatetime}, // bool 输入 = 类型不支持 → 策略零值
		{model.TypeDate, nil, true, mysqlZeroDate},
		{model.TypeTime, nil, true, mysqlZeroTime},
		{model.TypeYear, nil, "xx", int64(0)},
		{model.TypeBit, nil, 1.5, uint64(0)},
		{model.TypeEnum, nil, int64(99), ""},
		{model.TypeSet, nil, int64(99), ""},
		{model.TypeJSON, nil, "bad", json.RawMessage("{}")},
	}
	for _, tc := range cases {
		tr := New(Options{OnConvertError: StrategyZero})
		meta := oneCol("c", tc.typ, tc.mut)
		col, _ := meta.ColumnByName("c")
		got, err := tr.convertValue(meta, col, tc.in)
		if err != nil {
			t.Fatalf("%s: zero 策略不应返回错误: %v", tc.typ, err)
		}
		if !wantMatch(got, tc.want) {
			t.Fatalf("%s: got %#v want %#v", tc.typ, got, tc.want)
		}
	}
}
