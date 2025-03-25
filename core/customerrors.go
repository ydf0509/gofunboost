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
	Logger  *zap.Logger
}

// 实现基础的error接口
func (e FunboostError) Error() string {
	return fmt.Sprintf("Code: %d, Message: %s", e.Code, e.Message)
}

// 基础的日志记录方法
func (e FunboostError) Log() {
	e.Logger.Error(e.Error(),zap.StackSkip("stack",1))
}

type FunboostBrokerNetworkError struct {
	FunboostError
}

type FunboostRunError struct {
	FunboostError
}

// func main() {
// 	logger, _ := zap.NewDevelopment()
// 	sugar := logger.Sugar()

// 	err := &FunboostBrokerNetworkError{
// 		FunboostError: FunboostError{
// 			Message: "网络错误",
// 			Code:    10000,
// 			Logger:  sugar,
// 		},
// 	}

// 	if e, ok := interface{}(*err).(FunboostBrokerNetworkError); ok {
// 		sugar.Infof("网络错误: %s", e.Error())
// 	}

// }
