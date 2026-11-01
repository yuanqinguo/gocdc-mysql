package config

import (
	"strings"
	"testing"
)

// TestPipelineTransformOptions 04 §7 三旋钮：默认值 + 枚举校验。
func TestPipelineTransformOptions(t *testing.T) {
	// 省略 → ApplyDefaults 填安全默认
	c := validConfig()
	c.Pipeline.OnConvertError = ""
	c.Pipeline.DecimalAs = ""
	c.Pipeline.ZeroDatetimeAs = ""
	ApplyDefaults(&c)
	if errs := Validate(&c, "c1"); len(errs) != 0 {
		t.Fatalf("默认值应通过校验: %v", errs)
	}
	if c.Pipeline.OnConvertError != "null" {
		t.Fatalf("on_convert_error 默认应为 null，实际 %q", c.Pipeline.OnConvertError)
	}
	if c.Pipeline.DecimalAs != "string" {
		t.Fatalf("decimal_as 默认应为 string，实际 %q", c.Pipeline.DecimalAs)
	}
	if c.Pipeline.ZeroDatetimeAs != "null" {
		t.Fatalf("zero_datetime_as 默认应为 null，实际 %q", c.Pipeline.ZeroDatetimeAs)
	}

	// 非法值逐一报错
	for _, tc := range []struct{ field, value, sub string }{
		{"OnConvertError", "skip", "on_convert_error"},
		{"DecimalAs", "int", "decimal_as"},
		{"ZeroDatetimeAs", "zero", "zero_datetime_as"},
	} {
		errs := runV(t, func(c *Config) {
			switch tc.field {
			case "OnConvertError":
				c.Pipeline.OnConvertError = tc.value
			case "DecimalAs":
				c.Pipeline.DecimalAs = tc.value
			case "ZeroDatetimeAs":
				c.Pipeline.ZeroDatetimeAs = tc.value
			}
		})
		found := false
		for _, e := range errs {
			if strings.Contains(e.Error(), tc.sub) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s=%q 应报错（含 %q），实际 %v", tc.field, tc.value, tc.sub, errs)
		}
	}

	// 合法枚举全部通过
	for _, v := range []string{"null", "zero", "fail_event"} {
		if errs := runV(t, func(c *Config) { c.Pipeline.OnConvertError = v }); len(errs) != 0 {
			t.Fatalf("on_convert_error=%q 应合法: %v", v, errs)
		}
	}
	for _, v := range []string{"string", "float"} {
		if errs := runV(t, func(c *Config) { c.Pipeline.DecimalAs = v }); len(errs) != 0 {
			t.Fatalf("decimal_as=%q 应合法: %v", v, errs)
		}
	}
	for _, v := range []string{"null", "string"} {
		if errs := runV(t, func(c *Config) { c.Pipeline.ZeroDatetimeAs = v }); len(errs) != 0 {
			t.Fatalf("zero_datetime_as=%q 应合法: %v", v, errs)
		}
	}
}
