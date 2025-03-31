package core

// BrokerType 定义消息队列类型
type BrokerType string

const (
	// REDIS 使用Redis作为消息队列
	REDIS BrokerType = "redis"
	// RABBITMQ 使用RabbitMQ作为消息队列
	RABBITMQ BrokerType = "rabbitmq"
	// MEMORY 使用内存作为消息队列
	MEMORY BrokerType = "memory"
	// KAFKA 使用Kafka作为消息队列
	KAFKA BrokerType = "kafka"
	// SQLITE 使用SQLite作为消息队列
	SQLITE BrokerType = "sqlite"
)
