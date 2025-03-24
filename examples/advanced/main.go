// package main

// import (
// 	"fmt"
// 	"log"
// 	"time"

// 	"github.com/gofunboost/boost"
// 	"github.com/gofunboost/broker"
// )

// // 定义一个加法函数
// func add(x, y int) int {
// 	result := x + y
// 	log.Printf("计算 %d + %d = %d\n", x, y, result)
// 	time.Sleep(1 * time.Second)
// 	return result
// }

// // 定义一个打印函数
// func printValue(a interface{}) {
// 	fmt.Printf("打印值: %v\n", a)
// }

// // 定义一个可能会失败的函数
// func mayFail(id int) (string, error) {
// 	// 模拟随机失败
// 	if id%3 == 0 {
// 		return "", fmt.Errorf("处理ID %d 失败", id)
// 	}
// 	return fmt.Sprintf("成功处理ID %d", id), nil
// }

// func main() {
// 	// 创建使用Redis的Broker
// 	redisOptions := boost.BoostOptions{
// 		QueueName:     "redis_queue",
// 		ConsumeFunc:   add,
// 		BrokerKind:    broker.REDIS,
// 		ConnNum:       3,
// 		ConcurrentNum: 10,
// 		QPSLimit:      100,
// 		MaxRetries:    3,
// 		BrokerConfig: broker.Config{
// 			BrokerUrl: "localhost:6379",
// 			BrokerTransportOptions: map[string]interface{}{
// 				"db": 0,
// 			},
// 		},
// 	}

// 	// 创建使用RabbitMQ的Broker
// 	rabbitOptions := boost.BoostOptions{
// 		QueueName:     "rabbitmq_queue",
// 		ConsumeFunc:   printValue,
// 		BrokerKind:    broker.RABBITMQ,
// 		ConnNum:       2,
// 		ConcurrentNum: 5,
// 		QPSLimit:      50,
// 		MaxRetries:    3,
// 		BrokerConfig: broker.Config{
// 			BrokerUrl: "amqp://guest:guest@localhost:5672/",
// 		},
// 	}

// 	// 创建使用内存的Broker
// 	memoryOptions := boost.BoostOptions{
// 		QueueName:     "memory_queue",
// 		ConsumeFunc:   mayFail,
// 		BrokerKind:    broker.MEMORY,
// 		ConnNum:       1,
// 		ConcurrentNum: 3,
// 		QPSLimit:      0, // 不限制QPS
// 		MaxRetries:    3,
// 		BrokerConfig:  broker.Config{},
// 	}

// 	// 创建Broker实例
// 	redisBroker, err := boost.NewBroker(redisOptions)
// 	if err != nil {
// 		log.Fatalf("Failed to create Redis broker: %v", err)
// 	}

// 	rabbitBroker, err := boost.NewBroker(rabbitOptions)
// 	if err != nil {
// 		log.Fatalf("Failed to create RabbitMQ broker: %v", err)
// 	}

// 	memoryBroker, err := boost.NewBroker(memoryOptions)
// 	if err != nil {
// 		log.Fatalf("Failed to create Memory broker: %v", err)
// 	}

// 	// 启动消费
// 	go func() {
// 		if err := redisBroker.Consume(); err != nil {
// 			log.Printf("Redis broker consume error: %v", err)
// 		}
// 	}()

// 	go func() {
// 		if err := rabbitBroker.Consume(); err != nil {
// 			log.Printf("RabbitMQ broker consume error: %v", err)
// 		}
// 	}()

// 	go func() {
// 		if err := memoryBroker.Consume(); err != nil {
// 			log.Printf("Memory broker consume error: %v", err)
// 		}
// 	}()

// 	// 等待服务启动
// 	time.Sleep(2 * time.Second)

// 	// 推送消息到不同的队列
// 	log.Println("开始推送消息...")

// 	// 推送到Redis队列
// 	for i := 0; i < 5; i++ {
// 		if err := redisBroker.Push(i, i*2); err != nil {
// 			log.Printf("Failed to push to Redis broker: %v", err)
// 		}
// 	}

// 	// 推送到RabbitMQ队列
// 	for i := 0; i < 5; i++ {
// 		if err := rabbitBroker.Push(fmt.Sprintf("RabbitMQ消息 %d", i)); err != nil {
// 			log.Printf("Failed to push to RabbitMQ broker: %v", err)
// 		}
// 	}

// 	// 推送到内存队列
// 	for i := 0; i < 10; i++ {
// 		if err := memoryBroker.Push(i); err != nil {
// 			log.Printf("Failed to push to Memory broker: %v", err)
// 		}
// 	}

// 	log.Println("消息推送完成，等待处理...")

// 	// 阻塞进程，让所有协程池一直运行
// 	for {
// 		time.Sleep(10 * time.Second)
// 	}
// }
