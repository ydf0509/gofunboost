package core

import (
	"os"
	"path/filepath"

	// "sync"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// initLogger 初始化日志系统
func InitLogger(filename string,level zapcore.Level) (*zap.Logger,*zap.SugaredLogger, error) {
	// 创建日志目录
	logDir := filepath.Join("/", "logs")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil,nil, err
	}

	// 设置日志输出文件
	logFile := filepath.Join(logDir, filename)

	// 配置日志切割
	lumberJackLogger := &lumberjack.Logger{
		Filename:   logFile,
		MaxSize:    100, // MB
		MaxBackups: 5,
		MaxAge:     30, // days
		Compress:   true,
	}

	// 设置编码器配置
	fileEncoderConfig := zapcore.EncoderConfig{
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

	consoleEncoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalColorLevelEncoder,
		EncodeTime:     zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05"),
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// 创建文件输出核心
	fileCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(fileEncoderConfig),
		zapcore.AddSync(lumberJackLogger),
		level,
	)

	// 创建控制台输出核心
	consoleCore := zapcore.NewCore(
		zapcore.NewConsoleEncoder(consoleEncoderConfig),
		zapcore.AddSync(os.Stdout),
		level,
	)

	// 使用NewTee将两个Core合并
	core := zapcore.NewTee(fileCore, consoleCore)

	// 创建日志记录器
	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(0))
	sugar := logger.Sugar()

	return logger,sugar, nil
}

var Logger *zap.Logger
var Sugar *zap.SugaredLogger


func init() {
	Logger,Sugar, _ = InitLogger("gofunboost.log",zapcore.InfoLevel)
}
