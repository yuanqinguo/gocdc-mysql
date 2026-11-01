// logging.go：根 logger 的构建与全局存取（Setup/L/SetRoot/SetLevel/Level/
// Sync/ParseLevel）；包说明见 doc.go。

package logging

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config 日志配置。字段名与 03 §3.2 observability.log 的 json tag 一一对应
// （仅 level/json 两字段）；config 包加载后
// 映射到本结构——依赖方向 config → logging，禁止反向。
type Config struct {
	Level string `json:"level"` // debug|info|warn|error，空 = info
	JSON  bool   `json:"json"`  // true = JSON（生产）；false = console（开发）
}

var (
	root atomic.Pointer[zap.Logger]      // 全局根 logger，未初始化 = no-op
	lvl  atomic.Pointer[zap.AtomicLevel] // 根 logger 的动态级别（Setup 重建时整体替换）
)

func init() {
	root.Store(zap.NewNop())
	al := zap.NewAtomicLevelAt(zapcore.InfoLevel)
	lvl.Store(&al)
}

// L 返回当前根 logger；Setup 之前调用返回 no-op（审计/钩子等旁路调用安全），绝不 panic。
func L() *zap.Logger {
	if l := root.Load(); l != nil {
		return l
	}
	return zap.NewNop()
}

// SetRoot 替换全局根 logger（装配/测试注入任意 logger 用），返回旧值。
func SetRoot(l *zap.Logger) (old *zap.Logger) {
	if l == nil {
		l = zap.NewNop()
	}
	return root.Swap(l)
}

// SetLevel 动态调整根 logger 级别（11 §3 级别口径；进程日志级别由启动配置
// log.level 决定，本函数供嵌入方/测试运行时调级）。
func SetLevel(z zapcore.Level) { lvl.Load().SetLevel(z) }

// Level 返回根 logger 当前级别。
func Level() zapcore.Level { return lvl.Load().Level() }

// Sync 刷新底层写缓冲（优雅停机时调用；stdout 场景基本为 no-op，保留接口对称性）。
func Sync() { _ = root.Load().Sync() }

// ParseLevel 解析级别字符串；非法值报错（config 严格校验复用）。
func ParseLevel(s string) (zapcore.Level, error) {
	var l zapcore.Level
	if err := l.UnmarshalText([]byte(strings.ToLower(strings.TrimSpace(s)))); err != nil {
		return zapcore.InfoLevel, fmt.Errorf("非法日志级别 %q（可选 debug/info/warn/error）: %w", s, err)
	}
	return l, nil
}

// Setup 按配置构建根 logger 并设为全局，返回被替换的旧 logger。
// 输出写标准 stdout；JSON=true 用 JSON 编码（生产），否则 console（开发）。
func Setup(cfg Config) (*zap.Logger, error) {
	level, err := parseLevelOrDefault(cfg.Level)
	if err != nil {
		return nil, err
	}
	al := zap.NewAtomicLevelAt(level)

	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "ts"
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	var enc zapcore.Encoder
	if cfg.JSON {
		enc = zapcore.NewJSONEncoder(encCfg)
	} else {
		enc = zapcore.NewConsoleEncoder(encCfg)
	}

	logger := zap.New(zapcore.NewCore(enc, zapcore.AddSync(os.Stdout), al), zap.AddCaller())
	old := SetRoot(logger)
	lvl.Store(&al)
	return old, nil
}

// parseLevelOrDefault 空级别默认 info（03 §3.2 示例中 level 可缺省）。
func parseLevelOrDefault(s string) (zapcore.Level, error) {
	if strings.TrimSpace(s) == "" {
		return zapcore.InfoLevel, nil
	}
	return ParseLevel(s)
}
