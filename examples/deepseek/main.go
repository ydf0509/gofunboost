package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/redis/go-redis/v9"
	amqp "github.com/rabbitmq/amqp091-go"
)

// 定义消息结构
type Message struct {
	ID    string
	Body  []byte
	Topic string
}

// Broker 策略接口
type Broker interface {
	Subscribe(ctx context.Context, topic string, msgChan chan<- Message) error
	Publish(ctx context.Context, msg Message) error
	Close() error
}

// Consumer 消费主体
type Consumer struct {
	broker    Broker
	config    Config
	msgChan   chan Message
	waitGroup sync.WaitGroup
}

type Config struct {
	QueueName  string
	WorkerPool int
}

// 创建新的消费者
func NewConsumer(b Broker, cfg Config) *Consumer {
	return &Consumer{
		broker:  b,
		config:  cfg,
		msgChan: make(chan Message, cfg.WorkerPool),
	}
}

// 启动消费
func (c *Consumer) Start(ctx context.Context, topic string, handler func(Message) error) error {
	// 启动worker池
	for i := 0; i < c.config.WorkerPool; i++ {
		c.waitGroup.Add(1)
		go c.worker(handler)
	}

	// 启动消息订阅
	return c.broker.Subscribe(ctx, topic, c.msgChan)
}

// worker处理协程
func (c *Consumer) worker(handler func(Message) error) {
	defer c.waitGroup.Done()
	for msg := range c.msgChan {
		if err := handler(msg); err != nil {
			log.Printf("Message handle error: %v", err)
		}
	}
}

// 关闭连接
func (c *Consumer) Shutdown() error {
	close(c.msgChan)
	c.waitGroup.Wait()
	return c.broker.Close()
}

// Redis实现
type RedisBroker struct {
	client *redis.Client
	pubsub *redis.PubSub
}

func NewRedisBroker(addr string) *RedisBroker {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisBroker{client: client}
}

func (r *RedisBroker) Subscribe(ctx context.Context, topic string, msgChan chan<- Message) error {
	r.pubsub = r.client.Subscribe(ctx, topic)
	
	go func() {
		for msg := range r.pubsub.Channel() {
			message := Message{
				ID:    msg.Payload,
				Body:  []byte(msg.Payload),
				Topic: msg.Channel,
			}
			msgChan <- message
		}
	}()
	return nil
}

// ... Redis其他方法保持不变 ...

// RabbitMQ实现
type RabbitMQBroker struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewRabbitMQBroker(url string) (*RabbitMQBroker, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	
	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	
	return &RabbitMQBroker{
		conn:    conn,
		channel: channel,
	}, nil
}

func (r *RabbitMQBroker) Subscribe(ctx context.Context, queue string, msgChan chan<- Message) error {
	_, err := r.channel.QueueDeclare(
		queue,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,
	)
	if err != nil {
		return err
	}

	msgs, err := r.channel.Consume(
		queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			msg := Message{
				ID:    d.MessageId,
				Body:  d.Body,
				Topic: d.RoutingKey,
			}
			msgChan <- msg
			d.Ack(false)
		}
	}()
	return nil
}

// ... RabbitMQ其他方法保持不变 ...

// 使用示例
func main() {
	// 使用Redis
	redisBroker := NewRedisBroker("localhost:6379")
	redisConsumer := NewConsumer(redisBroker, Config{
		QueueName:  "my_topic",
		WorkerPool: 5, // 限制5个并发worker
	})

	// 使用RabbitMQ
	rabbitBroker, _ := NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	rabbitConsumer := NewConsumer(rabbitBroker, Config{
		QueueName:  "my_queue",
		WorkerPool: 10, // 限制10个并发worker
	})

	ctx := context.Background()

	// 启动Redis消费者
	go func() {
		redisConsumer.Start(ctx, "my_topic", func(msg Message) error {
			fmt.Printf("Redis worker[%d] received: %s\n", 
				// 获取当前goroutine的近似ID
				GetGoroutineID(), msg.Body)
			return nil
		})
	}()

	// 启动RabbitMQ消费者
	go func() {
		rabbitConsumer.Start(ctx, "my_queue", func(msg Message) error {
			fmt.Printf("RabbitMQ worker[%d] received: %s\n",
				GetGoroutineID(), msg.Body)
			return nil
		})
	}()

	// 生产测试消息
	for i := 0; i < 20; i++ {
		redisBroker.Publish(ctx, Message{
			Body:  []byte(fmt.Sprintf("redis msg %d", i)),
			Topic: "my_topic",
		})
		rabbitBroker.Publish(ctx, Message{
			Body:  []byte(fmt.Sprintf("rabbitmq msg %d", i)),
			Topic: "my_queue",
		})
	}

	// 保持程序运行
	select {}
}

// 辅助函数：获取近似goroutine ID（仅用于演示）
func GetGoroutineID() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	id := 0
	fmt.Sscanf(string(buf[:n]), "goroutine %d ", &id)
	return id
}