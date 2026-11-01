package logging

import (
	"context"
	"log/slog"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SlogHandler 把标准库 log/slog 适配到 zap（日志统一走 logging 入口）。
// 供第三方库（go-mysql 等）要求 *slog.Logger 时注入，其输出仍走统一日志管线，
// 事件名固定 "thirdparty"，来源库通过 attr "logger" 区分。
func SlogHandler(l *zap.Logger) slog.Handler {
	return &slogZap{log: l.With(zap.String("logger", "thirdparty"))}
}

type slogZap struct {
	log *zap.Logger
}

func (h *slogZap) Enabled(_ context.Context, level slog.Level) bool {
	switch {
	case level >= slog.LevelError:
		return h.log.Core().Enabled(zapcore.ErrorLevel)
	case level >= slog.LevelWarn:
		return h.log.Core().Enabled(zapcore.WarnLevel)
	case level >= slog.LevelInfo:
		return h.log.Core().Enabled(zapcore.InfoLevel)
	default:
		return h.log.Core().Enabled(zapcore.DebugLevel)
	}
}

func (h *slogZap) Handle(_ context.Context, r slog.Record) error {
	fields := make([]zap.Field, 0, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		fields = append(fields, zap.Any(a.Key, a.Value.Resolve()))
		return true
	})
	msg := r.Message
	switch {
	case r.Level >= slog.LevelError:
		h.log.Error(msg, fields...)
	case r.Level >= slog.LevelWarn:
		h.log.Warn(msg, fields...)
	case r.Level >= slog.LevelInfo:
		h.log.Info(msg, fields...)
	default:
		h.log.Debug(msg, fields...)
	}
	return nil
}

func (h *slogZap) WithAttrs(attrs []slog.Attr) slog.Handler {
	fields := make([]zap.Field, 0, len(attrs))
	for _, a := range attrs {
		fields = append(fields, zap.Any(a.Key, a.Value.Resolve()))
	}
	return &slogZap{log: h.log.With(fields...)}
}

func (h *slogZap) WithGroup(name string) slog.Handler {
	return &slogZap{log: h.log.Named(name)}
}

var _ slog.Handler = (*slogZap)(nil)
