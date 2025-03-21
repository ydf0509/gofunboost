package broker

import (
	"errors"
	"github.com/streadway/amqp"
)

// RabbitMQBroker 实现基于RabbitMQ的消息队列
type RabbitMQBroker struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

// NewRabbitMQBroker 创建一个新的RabbitMQ消息队列实例
func NewRabbitMQBroker(config Config) (*RabbitMQBroker, error) {
	if config.Address == "" {
		return nil, errors.New("RabbitMQ地址不能为空")
	}

	// 构建连接URL
	url := "amqp://"
	if config.Username != "" {
		url += config.Username
		if config.Password != "" {
			url += ":" + config.Password
		}
		url += "@"
	}
	url += config.Address

	// 连接到RabbitMQ服务器
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	// 创建通道
	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &RabbitMQBroker{
		conn:    conn,
		channel: channel,
	}, nil
}

// Push 将任务推送到RabbitMQ队列
func (r *RabbitMQBroker) Push(queueName string, data []byte) error {
	// 确保队列存在
	_, err := r.channel.QueueDeclare(
		queueName, // 队列名
		true,      // 持久化
		false,     // 不自动删除
		false,     // 非排他性
		false,     // 非阻塞
		nil,       // 额外参数
	)
	if err != nil {
		return err
	}

	// 发布消息
	return r.channel.Publish(
		"",        // 交换机
		queueName, // 路由键
		false,     // 强制发布
		false,     // 立即发布
		amqp.Publishing{
			ContentType: "application/octet-stream",
			Body:        data,
		},
	)
}

// Pop 从RabbitMQ队列中获取任务
func (r *RabbitMQBroker) Pop(queueName string) ([]byte, error) {
	// 确保队列存在
	_, err := r.channel.QueueDeclare(
		queueName, // 队列名
		true,      // 持久化
		false,     // 不自动删除
		false,     // 非排他性
		false,     // 非阻塞
		nil,       // 额外参数
	)
	if err != nil {
		return nil, err
	}

	// 获取消息
	msg, ok, err := r.channel.Get(
		queueName, // 队列名
		true,      // 自动确认
	)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil // 队列为空
	}

	return msg.Body, nil
}

// Consume 消费RabbitMQ队列中的任务
func (r *RabbitMQBroker) Consume(queueName string, handler func([]byte) error) error {
	// 确保队列存在
	_, err := r.channel.QueueDeclare(
		queueName, // 队列名
		true,      // 持久化
		false,     // 不自动删除
		false,     // 非排他性
		false,     // 非阻塞
		nil,       // 额外参数
	)
	if err != nil {
		return err
	}

	// 获取消费通道
	msgs, err := r.channel.Consume(
		queueName, // 队列名
		"",       // 消费者标签
		false,     // 不自动确认
		false,     // 非排他性
		false,     // 不阻塞
		false,     // 不等待服务器确认
		nil,       // 额外参数
	)
	if err != nil {
		return err
	}

	// 处理消息
	for msg := range msgs {
		err := handler(msg.Body)
		if err != nil {
			// 处理失败，拒绝消息并重新入队
			r.channel.Reject(msg.DeliveryTag, true)
			continue
		}

		// 处理成功，确认消息
		r.channel.Ack(msg.DeliveryTag, false)
	}

	return nil
}

// Close 关闭RabbitMQ连接
func (r *RabbitMQBroker) Close() error {
	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}