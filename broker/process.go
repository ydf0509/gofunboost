package broker

import (
	"reflect"

	"go.uber.org/zap"
)

// processMessage 处理消息
func (b *Broker) processMessage(args []interface{}, acker Acker) {
	// 获取函数的反射值
	funcValue := reflect.ValueOf(b.ConsumeFunc)
	funcType := funcValue.Type()

	// 检查参数数量是否匹配
	if len(args) != funcType.NumIn() {
		b.Logger.Error("Parameter count mismatch",
			zap.Int("expected", funcType.NumIn()),
			zap.Int("got", len(args)))
		return
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
				b.Logger.Error("Parameter type mismatch",
					zap.Int("parameter", i),
					zap.String("expected", expectedType.String()),
					zap.String("got", argType.String()))
				return
			}
		}

		in[i] = argValue
	}

	// 调用函数
	results := funcValue.Call(in)

	// 确认消息已处理
	if acker != nil {
		if err := acker.Ack(); err != nil {
			b.Logger.Error("Failed to ack message", zap.Error(err))
		}
	}

	// 处理返回值
	if len(results) > 0 {
		result := results[0].Interface()
		b.Logger.Info("Task executed successfully",
			zap.Any("result", result),
			zap.Any("args", args))
	} else {
		b.Logger.Info("Task executed successfully with no result",
			zap.Any("args", args))
	}
}

// // NewBroker 创建一个新的Broker
// func NewBroker(options interface{}) (BaseBroker, error) {
// 	// 将在boost包中实现
// 	return nil, ErrUnsupportedBrokerType
// }
