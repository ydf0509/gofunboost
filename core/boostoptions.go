package core

import (
	"reflect"

	"go.uber.org/zap"
)

// BoostOptions 定义Boost选项
type BoostOptions struct {
	QueueName     string
	ConsumeFunc   interface{}
	BrokerKind    BrokerType
	ConnNum       int
	ConcurrentNum int
	QPSLimit      float64
	MaxRetries    int
	BrokerConfig  Config
	Logger        *zap.Logger
}

// Config 定义Broker配置
type Config struct {
	BrokerUrl              string                 `json:"broker_url"`
	BrokerTransportOptions map[string]interface{} `json:"broker_transport_options"`
}

func MergeBoostOptions(reWriteOptions BoostOptions, baseOption *BoostOptions) BoostOptions {
	newOptions := BoostOptions{}
	if baseOption == nil {
		return reWriteOptions
	}
	newOptions = *baseOption

	// reWriteOptions 中非0值合并到 newOptions
	v := reflect.ValueOf(reWriteOptions)
	for i := 0; i < v.NumField(); i++ {
		fieldValue := v.Field(i)
		if !reflect.DeepEqual(fieldValue.Interface(), reflect.Zero(fieldValue.Type()).Interface()) {
			reflect.ValueOf(&newOptions).Elem().Field(i).Set(fieldValue)
		}
	}
	return newOptions
}
