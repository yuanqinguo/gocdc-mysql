package transform

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"strconv"

	"gocdc/internal/model"
)

// masker 脱敏算法接口（10 §1 算法集）。输入为已转换的值，输出字符串或 nil。
type masker interface {
	Apply(v any) any
}

// buildMasker 编译单个脱敏规格；未知算法返回错误（配置校验拦截在先，此处兜底）。
func buildMasker(spec model.MaskSpec) (masker, error) {
	switch spec.Type {
	case model.MaskFixed:
		v := spec.Value
		if v == "" {
			v = "***"
		}
		return fixedMasker(v), nil
	case model.MaskHashMD5:
		return &hashMasker{algo: md5.New, salt: spec.Salt}, nil
	case model.MaskHashSHA256:
		return &hashMasker{algo: sha256.New, salt: spec.Salt}, nil
	case model.MaskPhone:
		return phoneMasker{}, nil
	case model.MaskEmail:
		return emailMasker{}, nil
	case model.MaskTruncate:
		if spec.Keep < 1 {
			return nil, fmt.Errorf("truncate 算法须配 keep ≥1")
		}
		return truncateMasker(spec.Keep), nil
	case model.MaskNull:
		return nullMasker{}, nil
	default:
		return nil, fmt.Errorf("未知脱敏算法 %q", spec.Type)
	}
}

type fixedMasker string

func (m fixedMasker) Apply(any) any { return string(m) }

// hashMasker hex 摘要；盐从规则编译期已解析的 spec.Salt 注入（10 §5）。
type hashMasker struct {
	algo func() hash.Hash
	salt string
}

func (m *hashMasker) Apply(v any) any {
	h := m.algo()
	h.Write([]byte(m.salt + maskInput(v)))
	return hex.EncodeToString(h.Sum(nil))
}

// phoneMasker 保留前 3 后 4：138****1234；长度不足 8 全长打码（确定性规则）。
type phoneMasker struct{}

func (phoneMasker) Apply(v any) any {
	r := []rune(maskInput(v))
	if len(r) < 8 {
		return stars(len(r))
	}
	return string(r[:3]) + "****" + string(r[len(r)-4:])
}

// emailMasker 保留首字符与域名：a***@example.com；无 @ 视为非邮箱，整体打码。
type emailMasker struct{}

func (emailMasker) Apply(v any) any {
	s := maskInput(v)
	for i := 0; i < len(s); i++ {
		if s[i] == '@' {
			return s[:1] + "***" + s[i:]
		}
	}
	return "***"
}

type truncateMasker int

func (m truncateMasker) Apply(v any) any {
	r := []rune(maskInput(v))
	if len(r) <= int(m) {
		return string(r)
	}
	return string(r[:int(m)]) + "..."
}

type nullMasker struct{}

func (nullMasker) Apply(any) any { return nil }

func stars(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = '*'
	}
	return string(b)
}

// maskInput 把任意已转换值转成字符串形态参与脱敏（数字/布尔按 %v）。
func maskInput(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	case model.RawMessage:
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
