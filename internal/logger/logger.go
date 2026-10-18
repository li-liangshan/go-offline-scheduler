package logger

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	L *zap.Logger
	S *zap.SugaredLogger
)

func InitLogger() {
	_ = InitLoggerWithLevel("info")
}

func InitLoggerWithLevel(level string) error {
	parsed, err := zapcore.ParseLevel(level)
	if err != nil {
		return fmt.Errorf("parse log level %q: %w", level, err)
	}
	config := zap.NewProductionConfig()
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.Level = zap.NewAtomicLevelAt(parsed)
	L, err = config.Build()
	if err != nil {
		return err
	}
	S = L.Sugar()
	return nil
}

func Sync() {
	_ = L.Sync()
}
