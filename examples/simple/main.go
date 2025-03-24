package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gofunboost/boost"
	"github.com/gofunboost/broker"
)

// 定义一个加法函数
func add(x, y int) int {
	result := x + y
	log.Printf("计算 %d + %d = %d\n", x, y, result)
	time.Sleep(1 * time.Second)
	return result
}

// 定义一个打印函数
func printValue(a interface{}) {
	fmt.Printf("打印值: %v\n", a)
}

func main() {
	// 创建加法函数的Broker
	addOptions := boost.BoostOptions{
		QueueName:     "queue_test2",
		ConsumeFunc:   add,
		BrokerKind:    broker.REDIS,
		ConnNum:       5,
		ConcurrentNum: 50,
		QPSLimit:      500,
		MaxRetries:    3,
		BrokerConfig: broker.Config{
			BrokerUrl: "localhost:6379",
			BrokerTransportOptions: map[string]interface{}{
				"special1": 123,
			},
		},
	}

	addBooster, err := boost.NewBroker(addOptions)
	if err != nil {
		log.Fatalf("Failed to create add booster: %v", err)
	}

	// 创建打印函数的Broker
	printValueOptions := boost.BoostOptions{
		QueueName:     "queue_test33",
		ConsumeFunc:   printValue,
		BrokerKind:    broker.REDIS,
		ConnNum:       5,
		ConcurrentNum: 20,
		QPSLimit:      500,
		MaxRetries:    3,
		BrokerConfig: broker.Config{
			BrokerUrl: "localhost:6379",
			BrokerTransportOptions: map[string]interface{}{
				"special1": 123,
				"special2": "aaaa",
			},
		},
	}

	printValueBooster, err := boost.NewBroker(printValueOptions)
	if err != nil {
		log.Fatalf("Failed to create print value booster: %v", err)
	}

	// 启动消费
	go func() {
		if err := addBooster.Consume(); err != nil {
			log.Printf("Add booster consume error: %v", err)
		}
	}()

	go func() {
		if err := printValueBooster.Consume(); err != nil {
			log.Printf("Print value booster consume error: %v", err)
		}
	}()

	// 推送消息
	for i := 0; i < 10; i++ {
		if err := addBooster.Push(i, i*2); err != nil {
			log.Printf("Failed to push to add booster: %v", err)
		}

		if err := printValueBooster.Push(fmt.Sprintf("hello world %d", i)); err != nil {
			log.Printf("Failed to push to print value booster: %v", err)
		}

		time.Sleep(500 * time.Millisecond)
	}

	// 阻塞进程，让所有协程池一直运行
	for {
		time.Sleep(10 * time.Second)
	}
}
