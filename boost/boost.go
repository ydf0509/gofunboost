package boost

import (
	"github.com/gofunboost/broker"
	"github.com/gofunboost/concurrentpool"
)


// NewBroker 创建一个新的Broker
func NewBroker(options BoostOptions) (broker.BaseBroker, error) {
	// 初始化日志
	logger, err := initLogger()
	if err != nil {
		return nil, err
	}

	// 创建协程池
	pool := concurrentpool.NewGoEasyPool(options.ConcurrentNum)

	// 创建Broker
	brokerInstance := &broker.Broker{
		QueueName:     options.QueueName,
		ConsumeFunc:   options.ConsumeFunc,
		BrokerKind:    options.BrokerKind,
		ConnNum:       options.ConnNum,
		ConcurrentNum: options.ConcurrentNum,
		QPSLimit:      options.QPSLimit,
		MaxRetries:    options.MaxRetries,
		BrokerConfig:  options.BrokerConfig,
		Logger:        logger,
		Pool:          pool,
	}

	// 根据BrokerKind创建具体的Broker实现
	switch options.BrokerKind {
	case broker.REDIS:
		return broker.NewRedisBroker(brokerInstance)
	case broker.RABBITMQ:
		return broker.NewRabbitMQBroker(brokerInstance)
	case broker.MEMORY:
		return broker.NewMemoryBroker(brokerInstance)
	default:
		return nil, broker.ErrUnsupportedBrokerType
	}
}
