package task

import (
	"encoding/json"
	"errors"
	"reflect"
	"time"
)

// Task 表示一个任务
type Task struct {
	// 函数名称
	FuncName string `json:"func_name"`
	
	// 函数参数
	Args []interface{} `json:"args"`
	
	// 创建时间
	CreateTime time.Time `json:"create_time"`
	
	// 重试次数
	RetryCount int `json:"retry_count"`
	
	// 最大重试次数
	MaxRetries int `json:"max_retries"`
}

// NewTask 创建一个新任务
func NewTask(funcName string, args []interface{}, maxRetries int) *Task {
	return &Task{
		FuncName:   funcName,
		Args:       args,
		CreateTime: time.Now(),
		RetryCount: 0,
		MaxRetries: maxRetries,
	}
}

// Serialize 将任务序列化为JSON
func (t *Task) Serialize() ([]byte, error) {
	return json.Marshal(t)
}

// Deserialize 从JSON反序列化任务
func Deserialize(data []byte) (*Task, error) {
	var task Task
	err := json.Unmarshal(data, &task)
	return &task, err
}

// CanRetry 判断任务是否可以重试
func (t *Task) CanRetry() bool {
	return t.RetryCount < t.MaxRetries
}

// IncrementRetry 增加重试次数
func (t *Task) IncrementRetry() {
	t.RetryCount++
}

// CallFunction 调用函数并返回结果
func CallFunction(fn interface{}, args []interface{}) (interface{}, error) {
	// 获取函数的反射值
	fnValue := reflect.ValueOf(fn)
	
	// 检查是否为函数
	if fnValue.Kind() != reflect.Func {
		return nil, errors.New("不是一个有效的函数")
	}
	
	// 检查参数数量是否匹配
	fnType := fnValue.Type()
	if fnType.NumIn() != len(args) {
		return nil, errors.New("参数数量不匹配")
	}
	
	// 准备参数
	in := make([]reflect.Value, len(args))
	for i, arg := range args {
		// 获取期望的参数类型
		expectedType := fnType.In(i)
		
		// 将参数转换为期望的类型
		argValue := reflect.ValueOf(arg)
		if argValue.Type().ConvertibleTo(expectedType) {
			in[i] = argValue.Convert(expectedType)
		} else {
			return nil, errors.New("参数类型不匹配")
		}
	}
	
	// 调用函数
	out := fnValue.Call(in)
	
	// 处理返回值
	if len(out) == 0 {
		return nil, nil
	}
	
	// 返回第一个返回值
	result := out[0].Interface()
	
	// 检查是否有错误返回值
	if len(out) > 1 && !out[1].IsNil() && out[1].Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return result, out[1].Interface().(error)
	}
	
	return result, nil
}