package boost

// import (
// 	"github.com/gofunboost/broker"
// 	"github.com/gofunboost/core"
// )

// // NewBroker 创建一个新的Broker
// func NewBrokerWithBrokerType(options core.BoostOptions) (broker.Broker, error) {
// 	// 根据BrokerKind创建具体的Broker实现
// 	switch options.BrokerKind {
// 	case core.REDIS:
// 		return broker.NewRedisBroker(options), nil
// 	// case core.RABBITMQ:
// 	// 	return broker.NewRabbitMQBroker(brokerInstance)
// 	// case broker.MEMORY:
// 	// 	return broker.NewMemoryBroker(brokerInstance)
// 	default:
// 		return nil, broker.ErrUnsupportedBrokerType
// 	}
// }
