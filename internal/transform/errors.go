package transform

import (
	"errors"
	"fmt"
)

// ErrConvert 值转换失败哨兵（哨兵错误定义在各包 errors.go）。
// 用 errors.As 取出 *ConvertError 读取上下文。
var ErrConvert = errors.New("transform: 值转换失败")

// 转换失败的机器可读原因（ConvertError.Reason 取值；不含原始值——
// 红线：错误文本禁止携带未脱敏行列值）。
const (
	ReasonEnumIndex     = "enum_index_out_of_range" // 枚举索引越界
	ReasonSetMask       = "set_mask_invalid"        // SET 位掩码无法解析
	ReasonParseInt      = "parse_int_failed"        // 整型文本解析失败
	ReasonParseUint     = "parse_uint_failed"       // 无符号整型文本解析失败
	ReasonParseFloat    = "parse_float_failed"      // 浮点文本解析失败
	ReasonInvalidJSON   = "json_not_valid_text"     // JSON 列内容不是合法 JSON 文本
	ReasonUnsupported   = "unsupported_input_type"  // 输入 Go 类型与列类别组合不支持
	ReasonCharsetBroken = "charset_decode_failed"   // 字符列字节无法按声明字符集解码
	ReasonColumnCount   = "column_count_mismatch"   // 行列数与表结构不符（DDL 漂移/row image 非 FULL）
	ReasonNoPrimaryKey  = "no_primary_key"          // 表无主键且规则未配 order_by_column
	ReasonBadRow        = "row_shape_invalid"       // 像缺失/主键值缺失等行形态错误
)

// 包级哨兵错误（包装进 ConvertError.Err）。
var (
	errNilMeta  = errors.New("TableMeta 为空")
	errNoPK     = errors.New("表无主键且规则未配 order_by_column，无法寻址")
	errPKColumn = errors.New("主键列不在表结构中")
	errPKNil    = errors.New("主键值缺失或为 NULL")
)

// ConvertError 单列转换失败上下文。
//
// 处置由 pipeline 按 on_convert_error 策略决定（04 §7）；strategy=fail_event 时
// 本错误沿调用链上抛，pipeline 归类为不可重试（立即 PAUSED + 告警）。
type ConvertError struct {
	Schema, Table string
	Column        string
	Category      string // 列类别（04 §7 行）
	Reason        string // 上方 Reason* 常量
	Strategy      string // 命中失败时配置的策略（null/zero/fail_event）
	Err           error  // 底层错误（如 strconv 错误）
}

func (e *ConvertError) Error() string {
	return fmt.Sprintf("%v: %s.%s 列 %s（类别 %s, 原因 %s, 策略 %s）",
		ErrConvert, e.Schema, e.Table, e.Column, e.Category, e.Reason, e.Strategy)
}

// Unwrap 多重解包：错误链同时含 ErrConvert 哨兵（供上游 errors.Is 判类）
// 与底层原因（如 strconv 错误）。
func (e *ConvertError) Unwrap() []error {
	if e.Err != nil {
		return []error{ErrConvert, e.Err}
	}
	return []error{ErrConvert}
}

// IsFailEvent 是否为 fail_event 策略（pipeline 据此分类不可重试）。
func (e *ConvertError) IsFailEvent() bool { return e.Strategy == StrategyFailEvent }

// errImage 像与事件类型不匹配（如 INSERT 缺 after）。
func errImage(eventType, want string) error {
	return &ConvertError{Reason: ReasonBadRow, Strategy: StrategyNull,
		Err: fmt.Errorf("%s 事件缺 %s 像", eventType, want)}
}

// errColumnCount 行列数与表结构不符（schema 漂移或 binlog_row_image 非 FULL）。
func errColumnCount(have, want int) error {
	return fmt.Errorf("行列数 %d 与表结构列数 %d 不符（检查 binlog_row_image=FULL 与 DDL 刷新）", have, want)
}
