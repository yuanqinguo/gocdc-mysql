package transform

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"gocdc/internal/model"
)

func mustMask(t *testing.T, spec model.MaskSpec) masker {
	t.Helper()
	m, err := buildMasker(spec)
	if err != nil {
		t.Fatalf("buildMasker(%+v): %v", spec, err)
	}
	return m
}

func TestMask_Golden(t *testing.T) {
	md5Of := func(s string) string {
		sum := md5.Sum([]byte(s))
		return hex.EncodeToString(sum[:])
	}
	shaOf := func(s string) string {
		sum := sha256.Sum256([]byte(s))
		return hex.EncodeToString(sum[:])
	}

	cases := []struct {
		name string
		spec model.MaskSpec
		in   any
		want any
	}{
		{"fixed/默认星号", model.MaskSpec{Type: model.MaskFixed}, "secret", "***"},
		{"fixed/自定义值", model.MaskSpec{Type: model.MaskFixed, Value: "[REDACTED]"}, 123, "[REDACTED]"},
		{"md5/无盐", model.MaskSpec{Type: model.MaskHashMD5}, "abc", md5Of("abc")},
		{"md5/加盐", model.MaskSpec{Type: model.MaskHashMD5, Salt: "s1"}, "abc", md5Of("s1abc")},
		{"sha256/加盐", model.MaskSpec{Type: model.MaskHashSHA256, Salt: "s1"}, "abc", shaOf("s1abc")},
		{"sha256/数字输入按十进制串", model.MaskSpec{Type: model.MaskHashSHA256}, int64(12345), shaOf("12345")},
		{"phone/标准11位", model.MaskSpec{Type: model.MaskPhone}, "13812345678", "138****5678"},
		{"phone/不足8全长打码", model.MaskSpec{Type: model.MaskPhone}, "12345", "*****"},
		{"phone/8位边界", model.MaskSpec{Type: model.MaskPhone}, "12345678", "123****5678"},
		{"email/标准", model.MaskSpec{Type: model.MaskEmail}, "alice@example.com", "a***@example.com"},
		{"email/无@整体打码", model.MaskSpec{Type: model.MaskEmail}, "not-an-email", "***"},
		{"truncate/超长截断", model.MaskSpec{Type: model.MaskTruncate, Keep: 4}, "abcdefgh", "abcd..."},
		{"truncate/等长不截", model.MaskSpec{Type: model.MaskTruncate, Keep: 8}, "abcdefgh", "abcdefgh"},
		{"truncate/短于保留", model.MaskSpec{Type: model.MaskTruncate, Keep: 32}, "short", "short"},
		{"truncate/中文按字符", model.MaskSpec{Type: model.MaskTruncate, Keep: 2}, "中文测试", "中文..."},
		{"null/置空", model.MaskSpec{Type: model.MaskNull}, "anything", nil},
		{"null/输入nil仍nil", model.MaskSpec{Type: model.MaskNull}, nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mustMask(t, tc.spec).Apply(tc.in)
			if got != tc.want {
				t.Fatalf("got %#v want %#v", got, tc.want)
			}
		})
	}
}

func TestMask_UnknownAlgorithm(t *testing.T) {
	if _, err := buildMasker(model.MaskSpec{Type: "rot13"}); err == nil {
		t.Fatal("未知算法应报错")
	}
	if _, err := buildMasker(model.MaskSpec{Type: model.MaskTruncate, Keep: 0}); err == nil {
		t.Fatal("truncate keep=0 应报错")
	}
}

// TestMask_UnknownTypeInRedactor 红线验证（10 §5）：算法不可用时 Redactor
// 宁可整列不打，也绝不输出原始值。
func TestMask_UnknownTypeInRedactor(t *testing.T) {
	rules := map[string]*model.Rule{
		"db1.t1": {Key: "db1.t1", Schema: "db1", Table: "t1",
			MaskColumns: map[string]model.MaskSpec{"phone": {Type: "bogus"}}},
	}
	r := NewRedactor(rules)
	out := r.Redact("db1", "t1", map[string]any{"phone": "13812345678", "name": "x"})
	if _, ok := out["phone"]; ok {
		t.Fatalf("算法不可用时该列必须被移除，实际 %#v", out)
	}
	if out["name"] != "x" {
		t.Fatalf("无规则列应原样保留，实际 %#v", out)
	}
}
