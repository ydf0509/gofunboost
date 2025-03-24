package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/streadway/amqp"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// RabbitMQMessage 定义RabbitMQ消息结构
type RabbitMQMessage struct {
	Args []interface{} `json:"args"`
}

// RabbitMQAcker 实现Acker接口
type RabbitMQAcker struct {
	Channel *amqp.Channel
	Tag     uint64
}

// Ack 确认消息已处理
func (a *RabbitMQAcker) Ack() error {
	return a.Channel.Ack(a.Tag, false)
}

// Nack 拒绝消息
func (a *RabbitMQAcker) Nack() error {
	return a.Channel.Nack(a.Tag, false, true) // 重新入队
}

// RabbitMQBroker 实现RabbitMQ消息队列
type RabbitMQBroker struct {
	*Broker
	connections []*amqp.Connection
	limiter     *rate.Limiter
}

// NewRabbitMQBroker 创建RabbitMQ Broker
func NewRabbitMQBroker(broker *Broker) (BaseBroker, error) {
	if broker.QueueName == "" {
		return nil, errors.New("queue name is required")
	}

	if broker.ConsumeFunc == nil {
		return nil, ErrInvalidConsumeFunc
	}

	// 创建QPS限制器
	var limiter *rate.Limiter
	if broker.QPSLimit > 0 {
		limiter = rate.NewLimiter(rate.Limit(broker.QPSLimit), broker.QPSLimit)
	}

	return &RabbitMQBroker{
		Broker:      broker,
		connections: make([]*amqp.Connection, 0, broker.ConnNum),
		limiter:     limiter,
	}, nil
}

// createRabbitMQConnection 创建RabbitMQ连接
func (r *RabbitMQBroker) createRabbitMQConnection() (*amqp.Connection, error) {
	conn, err := amqp.Dial(r.BrokerConfig.BrokerUrl)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// createRabbitMQChannel 创建RabbitMQ通道
func (r *RabbitMQBroker) createRabbitMQChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	// 声明队列
	_, err = ch.QueueDeclare(
		r.QueueName, // 队列名
		true,        // 持久化
		false,       // 自动删除
		false,       // 排他性
		false,       // 不等待
		nil,         // 参数
	)
	if err != nil {
		return nil, err
	}

	return ch, nil
}

// Consume 实现消费逻辑
func (r *RabbitMQBroker) Consume() error {
	r.Logger.Info("Starting RabbitMQ consumer",
		zap.String("queue", r.QueueName),
		zap.Int("conn_num", r.ConnNum),
		zap.Int("concurrent_num", r.ConcurrentNum))

	// 创建多个连接进行消费
	for i := 0; i < r.ConnNum; i++ {
		go func(connID int) {
			r.Logger.Info("Starting consumer connection", zap.Int("conn_id", connID))
			for {
				ctx := context.Background()
				err := r.ConsumeUsingOneConn(ctx)
				if err != nil {
					r.Logger.Error("Consumer connection failed",
						zap.Int("conn_id", connID),
						zap.Error(err))
					// 等待一段时间后重试
					time.Sleep(60 * time.Second)
				}
			}
		}(i)
	}

	return nil
}

// ConsumeUsingOneConn 使用一个连接消费消息
func (r *RabbitMQBroker) ConsumeUsingOneConn(ctx context.Context) error {
	// 创建RabbitMQ连接
	conn, err := r.createRabbitMQConnection()
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ connection: %w", err)
	}
	defer conn.Close()

	// 创建通道
	ch, err := r.createRabbitMQChannel(conn)
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ channel: %w", err)
	}
	defer ch.Close()

	// 设置QoS
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	r.Logger.Info("RabbitMQ connection established", zap.String("addr", r.BrokerConfig.BrokerUrl))

	// 消费消息
	msgs, err := ch.Consume(
		r.QueueName, // 队列名
		"",          // 消费者标签
		false,       // 自动确认
		false,       // 排他性
		false,       // 不等待
		false,       // 参数
		nil,         // 参数
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	// 持续消费消息
	for {
		// 检查上下文是否已取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				return errors.New("message channel closed")
			}

			// 如果设置了QPS限制，则等待令牌
			if r.limiter != nil {
				r.limiter.Wait(ctx)
			}

			// 解析消息
			var message RabbitMQMessage
			if err := json.Unmarshal(msg.Body, &message); err != nil {
				r.Logger.Error("Failed to unmarshal message",
					zap.Error(err),
					zap.ByteString("message", msg.Body))
				// 确认消息，避免阻塞队列
				ch.Ack(msg.DeliveryTag, false)
				continue
			}

			// 创建Acker
			acker := &RabbitMQAcker{
				Channel: ch,
				Tag:     msg.DeliveryTag,
			}

			// 提交任务到协程池
			r.Pool.Submit(func() {
				r.processMessage(message.Args, acker)
			})
		}
	}
}

// processMessage 处理消息
func (r *RabbitMQBroker) processMessage(args []interface{}, acker Acker) {
	// 在这里调用用户提供的消费函数
	// 使用反射调用函数，并传入参数
	// 这部分需要根据实际情况实现
	// ...

	// 确认消息已处理
	if err := acker.Ack(); err != nil {
		r.Logger.Error("Failed to ack message", zap.Error(err))
	}
}

// Push 推送消息到队列
func (r *RabbitMQBroker) Push(args ...interface{}) error {
	// 创建RabbitMQ连接
	conn, err := r.createRabbitMQConnection()
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ connection: %w", err)
	}
	defer conn.Close()

	// 创建通道
	ch, err := r.createRabbitMQChannel(conn)
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ channel: %w", err)
	}
	defer ch.Close()

	// 创建消息
	message := RabbitMQMessage{
		Args: args,
	}

	// 序列化消息
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// 重试逻辑
	var lastErr error
	for i := 0; i < r.MaxRetries; i++ {
		// 发布消息
		err = ch.Publish(
			"",          // 交换机
			r.QueueName, // 路由键
			false,       // 强制
			false,       // 立即
			amqp.Publishing{
				ContentType:  "application/json",
				Body:         data,
				DeliveryMode: amqp.Persistent, // 持久化
			},
		)

		if err == nil {
			return nil
		}

		lastErr = err
		r.Logger.Warn("Failed to push message to RabbitMQ, retrying",
			zap.Error(err),
			zap.Int("attempt", i+1),
			zap.Int("max_retries", r.MaxRetries))

		// 等待一段时间后重试
		time.Sleep(5 * time.Second)

		// 尝试重新创建连接和通道
		conn.Close()
		conn, err = r.createRabbitMQConnection()
		if err != nil {
			r.Logger.Error("Failed to recreate RabbitMQ connection", zap.Error(err))
			continue
		}

		ch, err = r.createRabbitMQChannel(conn)
		if err != nil {
			r.Logger.Error("Failed to recreate RabbitMQ channel", zap.Error(err))
			continue
		}
	}

	r.Logger.Error("Failed to push message to RabbitMQ after retries",
		zap.Error(lastErr),
		zap.Int("max_retries", r.MaxRetries))

	return fmt.Errorf("failed to push message after %d retries: %w", r.MaxRetries, lastErr)
}

// Close 关闭Broker
func (r *RabbitMQBroker) Close() error {
	// 关闭所有RabbitMQ连接
	for _, conn := range r.connections {
		if conn != nil {
			conn.Close()
		}
	}

	return nil
}
