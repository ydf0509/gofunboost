package core

import "go.uber.org/zap"

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
	Logger        *zap.SugaredLogger
}

// Config 定义Broker配置
type Config struct {
	BrokerUrl              string                 `json:"broker_url"`
	BrokerTransportOptions map[string]interface{} `json:"broker_transport_options"`
}
