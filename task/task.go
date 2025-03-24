

package task

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/gofunboost/broker"
	"go.uber.org/zap"
)

// 定义错误
var (
	ErrInvalidFunction  = errors.New("invalid function")
	ErrInvalidArguments = errors.New("invalid arguments")
)

// Task 定义任务结构体
type Task struct {
	Function interface{}
	Logger   *zap.Logger
}

// NewTask 创建一个新的任务
func NewTask(function interface{}, logger *zap.Logger) (*Task, error) {
	// 验证函数是否有效
	if function == nil {
		return nil, ErrInvalidFunction
	}

	// 检查是否是函数类型
	if reflect.TypeOf(function).Kind() != reflect.Func {
		return nil, ErrInvalidFunction
	}

	return &Task{
		Function: function,
		Logger:   logger,
	}, nil
}

// Execute 执行任务
func (t *Task) Execute(args []interface{}, acker broker.Acker) (interface{}, error) {
	// 获取函数的反射值
	funcValue := reflect.ValueOf(t.Function)
	funcType := funcValue.Type()

	// 检查参数数量是否匹配
	if len(args) != funcType.NumIn() {
		t.Logger.Error("Parameter count mismatch",
			zap.Int("expected", funcType.NumIn()),
			zap.Int("got", len(args)))
		return nil, fmt.Errorf("%w: expected %d arguments, got %d", ErrInvalidArguments, funcType.NumIn(), len(args))
	}

	// 准备参数
	in := make([]reflect.Value, len(args))
	for i, arg := range args {
		// 获取期望的参数类型
		expectedType := funcType.In(i)

		// 获取实际参数的值和类型
		argValue := reflect.ValueOf(arg)
		argType := argValue.Type()

		// 检查类型是否兼容
		if !argType.AssignableTo(expectedType) {
			// 尝试类型转换
			if argValue.CanConvert(expectedType) {
				argValue = argValue.Convert(expectedType)
			} else {
				t.Logger.Error("Parameter type mismatch",
					zap.Int("parameter", i),
					zap.String("expected", expectedType.String()),
					zap.String("got", argType.String()))
				return nil, fmt.Errorf("%w: parameter %d expected %s, got %s", ErrInvalidArguments, i, expectedType, argType)
			}
		}

		in[i] = argValue
	}

	// 调用函数
	results := funcValue.Call(in)

	// 确认消息已处理
	if acker != nil {
		if err := acker.Ack(); err != nil {
			t.Logger.Error("Failed to ack message", zap.Error(err))
		}
	}

	// 处理返回值
	if len(results) == 0 {
		return nil, nil
	}

	// 返回第一个结果
	return results[0].Interface(), nil
}