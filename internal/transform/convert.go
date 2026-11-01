package transform

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"

	"gocdc/internal/model"
)

// 列类别（04 §7 表的行；Column.Type 归一化后的归类）。
const (
	catSignedInt   = "signed_int"   // tinyint..bigint signed → int64
	catUnsignedInt = "unsigned_int" // tinyint..bigint unsigned → uint64
	catDecimal     = "decimal"      // decimal/numeric → string（默认）| float
	catFloat       = "float"        // float → float64
	catDouble      = "double"       // double/real → float64
	catDatetime    = "datetime"
	catTimestamp   = "timestamp"
	catDate        = "date"
	catTime        = "time"
	catYear        = "year"
	catBit         = "bit"
	catEnum        = "enum"
	catSet         = "set"
	catJSON        = "json"
	catString      = "string" // char/varchar/text 家族
	catBytes       = "bytes"  // binary/varbinary/blob 家族
	catPassthrough = ""       // 未知类型：原样透传（前向兼容）
)

// category 把规范化类型名映射到 04 §7 的转换类别。
func category(col *model.Column) string {
	switch col.Type {
	case model.TypeTinyint, model.TypeSmallint, model.TypeMediumint, model.TypeInt, model.TypeBigint:
		if col.Unsigned {
			return catUnsignedInt
		}
		return catSignedInt
	case model.TypeDecimal:
		return catDecimal
	case model.TypeFloat:
		return catFloat
	case model.TypeDouble:
		return catDouble
	case model.TypeDatetime:
		return catDatetime
	case model.TypeTimestamp:
		return catTimestamp
	case model.TypeDate:
		return catDate
	case model.TypeTime:
		return catTime
	case model.TypeYear:
		return catYear
	case model.TypeBit:
		return catBit
	case model.TypeEnum:
		return catEnum
	case model.TypeSet:
		return catSet
	case model.TypeJSON:
		return catJSON
	case model.TypeChar, model.TypeVarchar,
		model.TypeTinytext, model.TypeText, model.TypeMediumtext, model.TypeLongtext:
		return catString
	case model.TypeBinary, model.TypeVarbinary,
		model.TypeTinyblob, model.TypeBlob, model.TypeMediumblob, model.TypeLongblob:
		return catBytes
	default:
		return catPassthrough
	}
}

// MySQL 各时间类型的输出格式（04 §7：DATETIME/TIMESTAMP 格式化字符串、
// DATE "2006-01-02"、TIME "15:04:05"）。
const (
	mysqlTimeFormat   = "2006-01-02 15:04:05"
	mysqlTimeFracFmt  = "2006-01-02 15:04:05.999999"
	mysqlDateFormat   = "2006-01-02"
	mysqlZeroDate     = "0000-00-00"
	mysqlZeroDatetime = "0000-00-00 00:00:00"
	mysqlZeroTime     = "00:00:00"
)

// convertValue 单列值转换（04 §7 全类型表）。
//
// 输入值来自两条路径且统一在此收敛（04 §7 补充规则：全量/增量同一转换器）：
//   - binlog 路径（go-mysql RowsEvent）：int64/uint64/float32/float64/[]byte/
//     string/time.Time/time.Duration/nil（具体由 syncer 配置决定）
//   - 文本协议路径（database/sql，全量）：nil/[]byte/string/int64/uint64/float64/time.Time
//
// 返回值的 Go 类型封闭集合：nil/int64/uint64/float64/string/[]byte/json.RawMessage。
func (t *Transformer) convertValue(meta *model.TableMeta, col *model.Column, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	cat := category(col)
	switch cat {
	case catSignedInt:
		return t.convInt(meta, col, v)
	case catUnsignedInt:
		return t.convUint(meta, col, v)
	case catDecimal:
		return t.convDecimal(meta, col, v)
	case catFloat, catDouble:
		return t.convFloat(meta, col, v)
	case catDatetime, catTimestamp:
		return t.convDatetime(meta, col, v)
	case catDate:
		return t.convDate(meta, col, v)
	case catTime:
		return t.convTime(meta, col, v)
	case catYear:
		return t.convYear(meta, col, v)
	case catBit:
		return t.convBit(meta, col, v)
	case catEnum:
		return t.convEnum(meta, col, v)
	case catSet:
		return t.convSet(meta, col, v)
	case catJSON:
		return t.convJSON(meta, col, v)
	case catString:
		return t.convString(meta, col, v)
	case catBytes:
		return t.convBytes(meta, col, v)
	default: // 未知类型透传（04 §7 之外的类型不擅自转换）
		return v, nil
	}
}

// fail 统一处置转换失败（04 §7 on_convert_error 三策略）：
// 计数指标 → 按策略返回零值/nil 或上抛 fail_event 错误。
func (t *Transformer) fail(meta *model.TableMeta, col *model.Column, cat, reason string, cause error) (any, error) {
	t.countError(meta, col.Name, t.opts.OnConvertError)
	ce := &ConvertError{
		Schema: meta.Schema, Table: meta.Table, Column: col.Name,
		Category: cat, Reason: reason, Strategy: t.opts.OnConvertError, Err: cause,
	}
	switch t.opts.OnConvertError {
	case StrategyFailEvent:
		return nil, ce
	case StrategyZero:
		return zeroValue(cat), nil
	default: // StrategyNull
		return nil, nil
	}
}

// zeroValue 各类别的零值（策略 zero；显式定义，禁止静默 0 之外的臆造）。
func zeroValue(cat string) any {
	switch cat {
	case catSignedInt, catUnsignedInt, catYear:
		return int64(0)
	case catDecimal:
		return "0"
	case catFloat, catDouble:
		return float64(0)
	case catDatetime, catTimestamp:
		return mysqlZeroDatetime
	case catDate:
		return mysqlZeroDate
	case catTime:
		return mysqlZeroTime
	case catBit:
		return uint64(0)
	case catEnum, catSet, catString:
		return ""
	case catJSON:
		return model.RawMessage("{}")
	case catBytes:
		return []byte{}
	default:
		return nil
	}
}

func (t *Transformer) convInt(meta *model.TableMeta, col *model.Column, v any) (any, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case uint64:
		if x > uint64(1<<63-1) {
			return t.fail(meta, col, catSignedInt, ReasonParseInt, fmt.Errorf("uint64 %d 超出 int64", x))
		}
		return int64(x), nil
	case int:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case float64: // 文本协议驱动偶发形态：仅接受整数值
		if x == float64(int64(x)) {
			return int64(x), nil
		}
		return t.fail(meta, col, catSignedInt, ReasonParseInt, fmt.Errorf("float64 非整数值"))
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return t.fail(meta, col, catSignedInt, ReasonParseInt, err)
		}
		return n, nil
	case []byte:
		n, err := strconv.ParseInt(string(x), 10, 64)
		if err != nil {
			return t.fail(meta, col, catSignedInt, ReasonParseInt, err)
		}
		return n, nil
	default:
		return t.fail(meta, col, catSignedInt, ReasonUnsupported, typeErr(v))
	}
}

func (t *Transformer) convUint(meta *model.TableMeta, col *model.Column, v any) (any, error) {
	switch x := v.(type) {
	case uint64:
		return x, nil
	case int64, int, int8, int16, int32, uint8, uint16, uint32:
		// go-mysql 对整型一律按有符号解析（v1.14 UnsignedMap 仅用于 Dump）：
		// 无符号列收到负数 = 同位宽回绕，按列位宽重解释（bigint unsigned 最大值
		// 以 int64(-1) 交付 → &mask 还原 uint64max）。不做位宽掩码还原，unsigned
		// bigint 最大值会被当负数/越界处置（04 §7）。
		n := toInt64(v)
		if n < 0 {
			n &= int64(unsignedMask(col.Type))
		}
		return uint64(n), nil
	case float64:
		if x >= 0 && x == float64(uint64(x)) {
			return uint64(x), nil
		}
		return t.fail(meta, col, catUnsignedInt, ReasonParseUint, fmt.Errorf("float64 非非负整数值"))
	case string:
		n, err := strconv.ParseUint(x, 10, 64)
		if err == nil {
			return n, nil
		}
		// 04 §7 兜底：BIGINT UNSIGNED 超出 uint64（理论不可达，防御文本路径）→ 原样字符串（decimal 语义）
		if numErr(err) {
			return x, nil
		}
		return t.fail(meta, col, catUnsignedInt, ReasonParseUint, err)
	case []byte:
		n, err := strconv.ParseUint(string(x), 10, 64)
		if err == nil {
			return n, nil
		}
		if numErr(err) {
			return string(x), nil
		}
		return t.fail(meta, col, catUnsignedInt, ReasonParseUint, err)
	default:
		return t.fail(meta, col, catUnsignedInt, ReasonUnsupported, typeErr(v))
	}
}

func (t *Transformer) convDecimal(meta *model.TableMeta, col *model.Column, v any) (any, error) {
	// 默认 string（无损，避免 float64 精度丢失）；float 模式仅兼容旧消费方
	if t.opts.DecimalAs == DecimalAsFloat {
		switch x := v.(type) {
		case float64:
			return x, nil
		case float32:
			return float64(x), nil
		case string:
			f, err := strconv.ParseFloat(x, 64)
			if err != nil {
				return t.fail(meta, col, catDecimal, ReasonParseFloat, err)
			}
			return f, nil
		case []byte:
			f, err := strconv.ParseFloat(string(x), 64)
			if err != nil {
				return t.fail(meta, col, catDecimal, ReasonParseFloat, err)
			}
			return f, nil
		default:
			return t.fail(meta, col, catDecimal, ReasonUnsupported, typeErr(v))
		}
	}
	switch x := v.(type) {
	case string:
		return t.decimalString(meta, col, x)
	case []byte:
		return t.decimalString(meta, col, string(x))
	case float64: // 罕见：驱动已按浮点交付；无损还原为定点文本
		return strconv.FormatFloat(x, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32), nil
	default:
		return t.fail(meta, col, catDecimal, ReasonUnsupported, typeErr(v))
	}
}

// decimalString string 模式下也做数值性校验（ParseFloat 全集覆盖 MySQL decimal 文本）：
// 非数值文本说明数据/元数据错位，显式失败而不是把垃圾透传到 sink（04 §7：不允许静默）。
func (t *Transformer) decimalString(meta *model.TableMeta, col *model.Column, s string) (any, error) {
	if _, err := strconv.ParseFloat(s, 64); err != nil {
		return t.fail(meta, col, catDecimal, ReasonParseFloat, err)
	}
	return s, nil
}

func (t *Transformer) convFloat(meta *model.TableMeta, col *model.Column, v any) (any, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err != nil {
			return t.fail(meta, col, category(col), ReasonParseFloat, err)
		}
		return f, nil
	case []byte:
		f, err := strconv.ParseFloat(string(x), 64)
		if err != nil {
			return t.fail(meta, col, category(col), ReasonParseFloat, err)
		}
		return f, nil
	default:
		return t.fail(meta, col, category(col), ReasonUnsupported, typeErr(v))
	}
}

// convDatetime DATETIME/TIMESTAMP → 源时区格式化字符串（04 §7）。
// 字符串输入是服务端按其时区写好的文本（增量 binlog 与全量文本协议的常态），原样透传；
// time.Time 输入（ParseTime 开启 / parseTime DSN）显式转到 source.timezone 再格式化。
func (t *Transformer) convDatetime(meta *model.TableMeta, col *model.Column, v any) (any, error) {
	cat := category(col)
	switch x := v.(type) {
	case string:
		return t.zeroDateString(meta, col, cat, x, mysqlZeroDatetime)
	case []byte:
		return t.zeroDateString(meta, col, cat, string(x), mysqlZeroDatetime)
	case time.Time:
		if x.IsZero() {
			return t.zeroDatePolicy(meta, col, cat, mysqlZeroDatetime)
		}
		return formatLoc(x.In(t.loc()), mysqlTimeFormat, mysqlTimeFracFmt), nil
	case int64: // 防御：TIMESTAMP 原始秒
		return time.Unix(x, 0).In(t.loc()).Format(mysqlTimeFormat), nil
	case uint64:
		return time.Unix(int64(x), 0).In(t.loc()).Format(mysqlTimeFormat), nil
	default:
		return t.fail(meta, col, cat, ReasonUnsupported, typeErr(v))
	}
}

func (t *Transformer) convDate(meta *model.TableMeta, col *model.Column, v any) (any, error) {
	switch x := v.(type) {
	case string:
		return t.dateString(meta, col, x)
	case []byte:
		return t.dateString(meta, col, string(x))
	case time.Time:
		if x.IsZero() {
			return t.zeroDatePolicy(meta, col, catDate, mysqlZeroDate)
		}
		return x.In(t.loc()).Format(mysqlDateFormat), nil
	default:
		return t.fail(meta, col, catDate, ReasonUnsupported, typeErr(v))
	}
}

// dateString DATE 文本：非零日期透传，零日期走策略（与 DATETIME 同一套零值语义）。
func (t *Transformer) dateString(meta *model.TableMeta, col *model.Column, s string) (any, error) {
	d := truncateDate(s)
	if d == mysqlZeroDate {
		return t.zeroDatePolicy(meta, col, catDate, mysqlZeroDate)
	}
	return d, nil
}

// convTime TIME → "15:04:05"（binlog 原始 duration 已由解析层还原；负值保留符号，
// 超 24h 的小时数照实展开——MySQL TIME 语义就是 ±838:59:59）。
func (t *Transformer) convTime(meta *model.TableMeta, col *model.Column, v any) (any, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	case time.Duration:
		return formatDuration(x), nil
	case time.Time:
		return x.In(t.loc()).Format("15:04:05"), nil
	default:
		return t.fail(meta, col, catTime, ReasonUnsupported, typeErr(v))
	}
}

func (t *Transformer) convYear(meta *model.TableMeta, col *model.Column, v any) (any, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case uint64:
		return int64(x), nil
	case int, int8, int16, int32, uint8, uint16, uint32:
		return toInt64(v), nil
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return t.fail(meta, col, catYear, ReasonParseInt, err)
		}
		return n, nil
	case []byte:
		n, err := strconv.ParseInt(string(x), 10, 64)
		if err != nil {
			return t.fail(meta, col, catYear, ReasonParseInt, err)
		}
		return n, nil
	default:
		return t.fail(meta, col, catYear, ReasonUnsupported, typeErr(v))
	}
}

// convBit BIT(n) → uint64（04 §7：按位解码；binlog/文本协议都是小端字节序的位串）。
func (t *Transformer) convBit(meta *model.TableMeta, col *model.Column, v any) (any, error) {
	var b []byte
	switch x := v.(type) {
	case []byte:
		b = x
	case string:
		b = []byte(x)
	case uint64:
		return x, nil
	case int64:
		if x < 0 {
			return t.fail(meta, col, catBit, ReasonUnsupported, fmt.Errorf("bit 位串为负 %d", x))
		}
		return uint64(x), nil
	default:
		return t.fail(meta, col, catBit, ReasonUnsupported, typeErr(v))
	}
	if len(b) > 8 {
		return t.fail(meta, col, catBit, ReasonUnsupported, fmt.Errorf("bit 位串 %d 字节超过 64 位上限", len(b)))
	}
	var out uint64
	for i := len(b) - 1; i >= 0; i-- { // 小端
		out = out<<8 | uint64(b[i])
	}
	return out, nil
}

func (t *Transformer) convEnum(meta *model.TableMeta, col *model.Column, v any) (any, error) {
	switch x := v.(type) {
	case int64:
		// 1 基索引查表；越界（含 0 = 保留非法值）→ 显式失败而非静默产出空串（04 §7）
		if x < 1 || int(x) > len(col.EnumValues) {
			return t.fail(meta, col, catEnum, ReasonEnumIndex,
				fmt.Errorf("enum 索引 %d 越界（取值数 %d）", x, len(col.EnumValues)))
		}
		return col.EnumValues[x-1], nil
	case uint64:
		return t.convEnum(meta, col, int64(x))
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	default:
		return t.fail(meta, col, catEnum, ReasonUnsupported, typeErr(v))
	}
}

func (t *Transformer) convSet(meta *model.TableMeta, col *model.Column, v any) (any, error) {
	switch x := v.(type) {
	case int64:
		if x < 0 {
			return t.fail(meta, col, catSet, ReasonSetMask, fmt.Errorf("set 位掩码为负 %d", x))
		}
		var parts []string
		for i, s := range col.SetValues {
			if x&(1<<uint(i)) != 0 {
				parts = append(parts, s)
			}
		}
		// 高位含未知成员 → 元数据与数据不一致，显式失败（04 §7：不允许静默）
		if rest := x >> uint(len(col.SetValues)); rest != 0 {
			return t.fail(meta, col, catSet, ReasonSetMask,
				fmt.Errorf("set 位掩码含未知高位（成员数 %d）", len(col.SetValues)))
		}
		return joinSet(parts), nil
	case uint64:
		return t.convSet(meta, col, int64(x))
	case string:
		return x, nil // 文本协议路径已是逗号拼接串
	case []byte:
		return string(x), nil
	default:
		return t.fail(meta, col, catSet, ReasonUnsupported, typeErr(v))
	}
}

// convJSON JSON → 原样文本（04 §7）：不反序列化（避免 map 绑参导致整批失败）。
// 产出 model.RawMessage：消息编码直接嵌入 JSON 值，MySQL sink 按 []byte 绑参。
// 内容不是合法 JSON 文本（如 syncer 误配了 JSON 二进制原样透出）→ 显式失败。
func (t *Transformer) convJSON(meta *model.TableMeta, col *model.Column, v any) (any, error) {
	var s string
	switch x := v.(type) {
	case string:
		s = x
	case []byte:
		s = string(x)
	case model.RawMessage:
		s = string(x)
	default:
		return t.fail(meta, col, catJSON, ReasonUnsupported, typeErr(v))
	}
	if !json.Valid([]byte(s)) {
		return t.fail(meta, col, catJSON, ReasonInvalidJSON, fmt.Errorf("内容不是合法 JSON 文本"))
	}
	return model.RawMessage(s), nil
}

// convString 字符家族 → string。按 utf8 家族直接解码；字节不是合法 UTF-8
// 且声明字符集非 utf8 家族时显式失败（不静默 mojibake；非 utf8 库在启动检查告警）。
func (t *Transformer) convString(meta *model.TableMeta, col *model.Column, v any) (any, error) {
	var b []byte
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		b = x
	default:
		return t.fail(meta, col, catString, ReasonUnsupported, typeErr(v))
	}
	if utf8.Valid(b) {
		return string(b), nil
	}
	if isUTF8Charset(col.Charset) {
		// 声明 utf8 却出现坏字节：数据本身已损坏，仍按字节透传保真（转义由消息层处理）
		return string(b), nil
	}
	return t.fail(meta, col, catString, ReasonCharsetBroken,
		fmt.Errorf("字符集 %s 的字节不是合法 UTF-8", col.Charset))
}

// convBytes 二进制家族 → []byte（MySQL sink 直接绑参；MQ 侧 UTF-8/base64 决策在消息编码）。
func (t *Transformer) convBytes(meta *model.TableMeta, col *model.Column, v any) (any, error) {
	switch x := v.(type) {
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	default:
		return t.fail(meta, col, catBytes, ReasonUnsupported, typeErr(v))
	}
}

// ── 辅助 ─────────────────────────────────────────────────────────────

// zeroDateString 处理零日期策略（DATETIME/TIMESTAMP 字符串输入）。
func (t *Transformer) zeroDateString(meta *model.TableMeta, col *model.Column, cat, s, zero string) (any, error) {
	if len(s) >= 10 && s[:10] == mysqlZeroDate {
		return t.zeroDatePolicy(meta, col, cat, zero)
	}
	return s, nil
}

// zeroDatePolicy 零日期按配置：null（默认，+计数）或原样字符串（04 §7）。
func (t *Transformer) zeroDatePolicy(meta *model.TableMeta, col *model.Column, cat, zero string) (any, error) {
	if t.opts.ZeroDatetimeAs == ZeroDatetimeString {
		return zero, nil
	}
	t.countError(meta, col.Name, "zero_date") // 独立计数口径：零日期不算转换错误但需可见
	return nil, nil
}

// truncateDate 截掉 DATETIME 文本里的时间部分（DATE 列输入可能带 00:00:00）。
func truncateDate(s string) string {
	if len(s) >= 10 {
		return s[:10]
	}
	return s
}

// formatLoc 有小数秒用带分数格式，否则整秒（无损优先）。
func formatLoc(t time.Time, plain, frac string) string {
	if t.Nanosecond() == 0 {
		return t.Format(plain)
	}
	return t.Format(frac)
}

// formatDuration 渲染 MySQL TIME 语义的时长（小时可 >24、可负）。
func formatDuration(d time.Duration) string {
	sign := ""
	if d < 0 {
		sign = "-"
		d = -d
	}
	h := int64(d / time.Hour)
	m := int64(d/time.Minute) % 60
	s := int64(d/time.Second) % 60
	return fmt.Sprintf("%s%02d:%02d:%02d", sign, h, m, s)
}

func isUTF8Charset(cs string) bool {
	switch cs {
	case "", "utf8", "utf8mb3", "utf8mb4", "ascii":
		return true
	}
	return false
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	}
	return 0
}

// unsignedMask 无符号整型各类型位宽掩码（go-mysql 有符号回绕的重解释）。
func unsignedMask(typ string) uint64 {
	switch typ {
	case model.TypeTinyint:
		return 0xFF
	case model.TypeSmallint:
		return 0xFFFF
	case model.TypeMediumint:
		return 0xFF_FFFF
	case model.TypeInt:
		return 0xFFFF_FFFF
	default: // bigint / 未知
		return ^uint64(0)
	}
}

// numErr 是否为数值越界（ParseInt/ParseUint 的 ErrRange）——越界走兜底而非失败。
func numErr(err error) bool {
	ne, ok := err.(*strconv.NumError)
	return ok && ne.Err == strconv.ErrRange
}

func typeErr(v any) error {
	return fmt.Errorf("不支持的输入类型 %T", v)
}

func joinSet(parts []string) string {
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return parts[0]
	}
	out := parts[0]
	for _, p := range parts[1:] {
		out += "," + p
	}
	return out
}
