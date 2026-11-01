package config

import (
	"fmt"
	"os"
	"regexp"
)

// envNameRe 合法环境变量名。
var envNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// envRefRe ${VAR} 占位引用。
var envRefRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// ResolveEnv 递归遍历反序列化后的配置树，把字符串值中的 ${VAR} 替换为环境变量
// （03 §3.2：敏感字段 password/auth_token 等；不限于敏感字段，统一处理）。
// 引用了未设置的环境变量 → 报错并收集路径（fail-fast，绝不带空值静默运行）。
// 返回替换后的配置树（可直接 json.Marshal）。
func ResolveEnv(tree any) (any, []error) {
	var errs []error
	resolved := walkEnv(tree, "$", &errs)
	return resolved, errs
}

func walkEnv(v any, path string, errs *[]error) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, vv := range t {
			out[k] = walkEnv(vv, path+"."+k, errs)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, vv := range t {
			out[i] = walkEnv(vv, fmt.Sprintf("%s[%d]", path, i), errs)
		}
		return out
	case string:
		return expandEnvStr(t, path, errs)
	default:
		return v
	}
}

// expandEnvStr 替换字符串中的全部 ${VAR} 占位；未设置的变量记错误，占位保留原文
// （让后续校验输出携带上下文，便于定位）。
func expandEnvStr(s, path string, errs *[]error) string {
	if !envRefRe.MatchString(s) {
		return s
	}
	return envRefRe.ReplaceAllStringFunc(s, func(ref string) string {
		name := envRefRe.FindStringSubmatch(ref)[1]
		val, ok := os.LookupEnv(name)
		if !ok {
			*errs = append(*errs, fmt.Errorf("%s: 引用的环境变量 %s 未设置", path, name))
			return ref
		}
		return val
	})
}
