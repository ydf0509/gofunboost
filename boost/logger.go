package boost

import (
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// initLogger 初始化日志系统
func initLogger() (*zap.Logger, error) {
	// 创建日志目录
	logDir := "logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}

	// 设置日志输出文件
	logFile := filepath.Join(logDir, "gofunboost.log")

	// 配置日志切割
	lumberJackLogger := &lumberjack.Logger{
		Filename:   logFile,
		MaxSize:    100, // MB
		MaxBackups: 5,
		MaxAge:     30, // days
		Compress:   true,
	}

	// 设置编码器配置
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// 创建核心
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(lumberJackLogger),
		zapcore.InfoLevel,
	)

	// 创建日志记录器
	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))

	// 替换全局日志记录器
	zap.ReplaceGlobals(logger)

	return logger, nil
}

// GetLogger 获取日志记录器
func GetLogger() *zap.Logger {
	return zap.L()
}

// SyncLogger 同步日志记录器
func SyncLogger() {
	zap.L().Sync()
}
