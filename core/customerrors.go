package core

import (
	"fmt"

	"go.uber.org/zap"
)

type ErrorLog interface {
	Error() string
	Log()
	IsErrType(typex interface{}) bool
}

type FunboostError struct {
	Message string
	Code    int
	Cause   error
	Logger  *zap.Logger
	ErrName string
}

// 实现基础的error接口
func (e FunboostError) Error() string {
	return fmt.Sprintf("Code: %d, Message: %s", e.Code, e.Message)
}

func (e *FunboostError) Unwrap() error {
	return e.Cause
}

// 基础的日志记录方法
func (e FunboostError) Log() {

	e.Logger.Error(fmt.Sprintf("%v ",e.ErrName),
		zap.Error(e),            // 记录完整错误
		zap.NamedError("cause",e.Cause),      // 记录原始错误
		zap.String("message", e.Message),
		zap.Int("code", e.Code), // 单独记录字段
		// zap.Stack("stack"),      //调用栈
		zap.StackSkip("stack", 0),
	)
}

type BrokerNetworkError struct {
	FunboostError
}

type FunboostRunError struct {
	FunboostError
}

func NewBrokerNetworkError(message string, code int,cuase error, logger *zap.Logger) *BrokerNetworkError {
	return &BrokerNetworkError{
		FunboostError: FunboostError{
			Message: message,
			Code:    code,
			Cause:   cuase,
			Logger:  logger,
			ErrName: "BrokerNetworkError",
		},
	}
}

func NewFunboostRunError(message string, code int, cause error,logger *zap.Logger) *FunboostRunError {
	return &FunboostRunError{
		FunboostError: FunboostError{
			Message: message,
			Code:    code,
			Cause:   cause,
			Logger:  logger,
			ErrName: "FunboostRunError",
		},
	}
}

