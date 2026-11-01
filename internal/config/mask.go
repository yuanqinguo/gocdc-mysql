package config

import (
	"encoding/json"
	"fmt"
)

// 脱敏算法类型集（10 §1，内置可扩）。
const (
	MaskFixed      = "fixed" // 替换固定值（Value，默认 ***）
	MaskHashMD5    = "hash_md5"
	MaskHashSHA256 = "hash_sha256" // 可选 SaltEnv 加盐
	MaskPhone      = "phone"       // 138****1234
	MaskEmail      = "email"       // a***@example.com
	MaskTruncate   = "truncate"    // 保留前 Keep 字符 + ...
	MaskNull       = "null"
)

// MaskSpec 脱敏规则（10 §1：既接受内置算法名字符串，也接受对象形式）。
type MaskSpec struct {
	Type    string `json:"type"`
	Value   string `json:"value,omitempty"`    // fixed 的替换值
	Keep    int    `json:"keep,omitempty"`     // truncate 保留长度
	SaltEnv string `json:"salt_env,omitempty"` // hash_* 的盐环境变量名
}

// UnmarshalJSON 兼容两种形态：
//
//	"phone"                          → {Type:"phone"}
//	{"type":"truncate","keep":32}    → 原样
func (m *MaskSpec) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*m = MaskSpec{Type: s}
		return nil
	}
	type alias MaskSpec // 防递归
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return fmt.Errorf("mask_columns 值须为算法名或 {type,...} 对象: %w", err)
	}
	*m = MaskSpec(a)
	return nil
}

// validateMaskSpec 校验单个脱敏规格（10 §1 算法集）。
func validateMaskSpec(column string, m MaskSpec) error {
	switch m.Type {
	case MaskFixed, MaskPhone, MaskEmail, MaskNull:
		return nil
	case MaskHashMD5, MaskHashSHA256:
		if m.SaltEnv != "" && !envNameRe.MatchString(m.SaltEnv) {
			return fmt.Errorf("mask_columns[%s].salt_env %q 不是合法环境变量名", column, m.SaltEnv)
		}
		return nil
	case MaskTruncate:
		if m.Keep < 1 {
			return fmt.Errorf("mask_columns[%s].type=truncate 须配 keep ≥1", column)
		}
		return nil
	case "":
		return fmt.Errorf("mask_columns[%s].type 为空", column)
	default:
		return fmt.Errorf("mask_columns[%s].type %q 不在算法集（fixed/hash_md5/hash_sha256/phone/email/truncate/null）", column, m.Type)
	}
}
