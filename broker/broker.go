package broker

import "errors"

// BrokerKind 表示消息队列类型的枚举
// type BrokerKind string

// 支持的消息队列类型
const (
	REDIS string = "redis"
	RABBITMQ string = "rabbitmq"
	// 未来可以扩展更多的消息队列类型
)

// Broker 定义消息队列接口
type Broker interface {
	// Push 将任务推送到队列
	Push(queueName string, data []byte) error
	
	// Pop 从队列中获取任务
	Pop(queueName string) ([]byte, error)
	
	// Consume 消费队列中的任务
	Consume(queueName string, handler func([]byte) error) error
	
	// Close 关闭连接
	Close() error
}

// Config 消息队列配置
type Config struct {
	// 消息队列类型
	Kind string
	
	// 连接地址
	Address string
	
	// 用户名
	Username string
	
	// 密码
	Password string
	
	// 数据库索引（Redis专用）
	DB int
}

// NewBroker 创建一个新的消息队列实例
func NewBroker(config Config) (Broker, error) {
	// 根据消息队列类型创建对应的实例
	switch config.Kind {
	case REDIS:
		return NewRedisBroker(config)
	case RABBITMQ:
		return NewRabbitMQBroker(config)
	default:
		return nil, errors.New("不支持的消息队列类型")
	}
}