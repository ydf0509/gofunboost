package core

import (
	"fmt"
	"runtime"

	"go.uber.org/zap"
)

type ErrorLog interface {
	Error() string
	Log()
	// IsErrType(typex interface{}) bool
	GetErrName() string
}

type BaseError struct {
	Message string
	Code    int
	Cause   error
	Logger  *zap.Logger
	GetFileLine   func() string // 延迟获取文件和行号
	
	// ErrName string
}


// 实现基础的error接口
func (e BaseError) Error() string {
	// return fmt.Sprintf("Code: %d, Message: %s  ,File: %s, Line: %d, Cause: %v", e.Code, e.Message,e.File,e.Line, e.Cause)
	var causeStr string
    if e.Cause != nil {
        causeStr = fmt.Sprintf(", Cause: %v", e.Cause)
    }
    // return fmt.Sprintf("Code: %d, Message: %s, FileLine: %s, cause: %s", 
    //     e.Code, e.Message, e.GetFileLine(), causeStr)
	return fmt.Sprintf("Code: %d, Message: %s, cause: %s", 
    e.Code, e.Message, causeStr)
}

func (e *BaseError) Unwrap() error {
	return e.Cause
}

// 基础的日志记录方法
func (e BaseError) Log() {
	// _, file, line, _ := runtime.Caller(1)  // 获取调用栈信息
	// pc, file, line, _ := runtime.Caller(1)
    // fn := runtime.FuncForPC(pc)

	e.Logger.Error(fmt.Sprintf("%v ",e.GetErrName()),
		zap.Error(e),            // 记录完整错误
		zap.NamedError("cause",e.Cause),      // 记录原始错误
		zap.String("message", e.Message),
		zap.Int("code", e.Code), // 单独记录字段
		// zap.Stack("stack"),      //调用栈
		zap.StackSkip("stack", 0),
	)
}

func (e *BaseError) GetErrName() string {
	return "BaseError"
}


type BrokerNetworkError struct {
	BaseError
}

func (e *BrokerNetworkError) GetErrName() string {
	return "BrokerNetworkError"
}

type FunboostRunError struct {
	BaseError
}

func (e *FunboostRunError) GetErrName() string {
	return "FunboostRunError"
}

func NewBrokerNetworkError(message string, code int,cause error, logger *zap.Logger) ErrorLog {
	return NewError((*BrokerNetworkError)(nil),message, code, cause, logger)
}

func NewFunboostRunError(message string, code int, cause error,logger *zap.Logger) ErrorLog {
	return NewError((*FunboostRunError)(nil),message, code, cause, logger)
	// return NewError(FunboostRunError{},message, code, cause, logger) //这个不好
	// return &FunboostRunError{
	// 	FunboostBaseError: FunboostBaseError{
	// 		Message: message,
	// 		Code:    code,
	// 		Cause:   cause,
	// 		Logger:  logger,
	// 	},
	// }
}


// ... 保留其他代码不变 ...

// 统一错误创建入口（基于类型判断）
func NewError(errType error, message string, code int, cause error, logger *zap.Logger) ErrorLog {

    baseErr := BaseError{
        Message: message,
        Code:    code,
        Cause:   cause,
        Logger:  logger,
		GetFileLine: func() string {
            _, file, line, _ := runtime.Caller(7)
            return fmt.Sprintf("%s:%d", file, line)
        },
    }
    // 通过类型断言设置错误类型
    switch errType.(type) {
    case *BrokerNetworkError:
        return &BrokerNetworkError{BaseError: baseErr}
    case *FunboostRunError:
        return &FunboostRunError{BaseError: baseErr}
    default:
        return &baseErr
    }
}

// go 1.18 之前的版本使用反射的统一错误创建入口
// func NewError(errType ErrorLog, message string, code int, cause error, logger *zap.Logger) ErrorLog {
//     baseErr := FunboostBaseError{
//         Message: message,
//         Code:    code,
//         Cause:   cause,
//         Logger:  logger,
//     }

//     // 使用反射创建对应类型的实例
//     t := reflect.TypeOf(errType).Elem()
//     v := reflect.New(t).Interface().(ErrorLog)
//     reflect.ValueOf(v).Elem().FieldByName("FunboostBaseError").Set(reflect.ValueOf(baseErr))

//     return v
// }







// // go1.18 使用泛型的统一错误创建入口
// func NewError[T ErrorLog](errType T, message string, code int, cause error, logger *zap.Logger) T {
//     baseErr := FunboostBaseError{
//         Message: message,
//         Code:    code,
//         Cause:   cause,
//         Logger:  logger,
//     }

//     // 直接创建泛型类型T的实例
//     var t T
//     v := any(&t).(*T)
//     *v = &struct {
//         FunboostBaseError
//     }{
//         FunboostBaseError: baseErr,
//     }

//     return t
// }

// // 删除原有的 NewBrokerNetworkError 和 NewFunboostRunError 函数