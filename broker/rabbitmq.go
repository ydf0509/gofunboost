package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/streadway/amqp"
	"github.com/ydf0509/gofunboost/core"
	"go.uber.org/zap"
)

// RabbitMQBroker 实现RabbitMQ消息队列
type RabbitMQBroker struct {
	*BaseBroker
	conn    *amqp.Connection
	channel *amqp.Channel
}

func (b *RabbitMQBroker) createRabbitMQConnection() (*amqp.Connection, error) {
	conn, err := amqp.Dial(b.BrokerConfig.BrokerUrl)
	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Could not connect to RabbitMQ: %v", err), 0, err, b.Logger)
		err2.Log()
		return nil, err2
	}
	b.Sugar.Info("Connected to RabbitMQ")
	return conn, nil
}

func (b *RabbitMQBroker) createChannel() (*amqp.Channel, error) {
	ch, err := b.conn.Channel()
	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Could not create RabbitMQ channel: %v", err), 0, err, b.Logger)
		err2.Log()
		return nil, err2
	}

	// 声明队列
	_, err = ch.QueueDeclare(
		b.QueueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Could not declare RabbitMQ queue: %v", err), 0, err, b.Logger)
		err2.Log()
		return nil, err2
	}

	return ch, nil
}

func (b *RabbitMQBroker) newBrokerCustomInit() {
	b.Sugar.Infof("newRabbitMQBrokerCustomInit %v", b)
	var err error
	b.conn, err = b.createRabbitMQConnection()
	if err != nil {
		panic(err)
	}

	b.channel, err = b.createChannel()
	if err != nil {
		panic(err)
	}
}

func (b *RabbitMQBroker) impConsumeUsingOneConn() error {
	b.Logger.Info("RabbitMQ connection established", zap.String("addr", b.BrokerConfig.BrokerUrl))

	rabbitmq_channel, err := b.createChannel()
	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Could not create RabbitMQ channel: %v", err), 0, err, b.Logger)
		err2.Log()
		return err2
	}
	// 设置QoS
	err = rabbitmq_channel.Qos(
		5,     // prefetch count
		0,     // prefetch size (0 means no limit)
		false, // global
	)
	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Could not set RabbitMQ QoS: %v", err), 0, err, b.Logger)
		err2.Log()
		return err2
	}

	// 开始消费消息
	msgs, err := rabbitmq_channel.Consume(
		b.QueueName,
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Could not start consuming: %v", err), 0, err, b.Logger)
		err2.Log()
		return err2
	}

	ctx := context.Background()
	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-msgs:
			if !ok {
				err2 := core.NewBrokerNetworkError("RabbitMQ channel closed", 0, nil, b.Logger)
				err2.Log()
				return err2
			}

			// 反序列化消息
			msg := b.imp.Json2Message(string(d.Body))
			if msg == nil {
				b.Sugar.Error("Failed to deserialize message")
				d.Reject(false)
				continue
			}

			// 使用协程池处理消息
			b.Pool.Submit(func() {
				b.execute(&core.MessageWrapper{
					Msg: msg,
					ContextExtra: map[string]interface{}{
						"delivery": &d,
					},
				})
			})
		}
	}
}

func (b *RabbitMQBroker) impSendMsg(msg string) error {
	err := b.channel.Publish(
		"",          // exchange
		b.QueueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
			Timestamp:   time.Now(),
		},
	)

	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Failed to publish message to RabbitMQ: %v", err), 0, err, b.Logger)
		err2.Log()
		return err2
	}

	return nil
}

func (b *RabbitMQBroker) impAckMsg(msgWrapper *core.MessageWrapper) error {
	if d, ok := msgWrapper.ContextExtra["delivery"].(*amqp.Delivery); ok {
		err := d.Ack(false)
		if err != nil {
			err2 := core.NewBrokerNetworkError(fmt.Sprintf("Failed to ack message: %v", err), 0, err, b.Logger)
			err2.Log()
			return err2
		}
	}
	return nil
}
