package main

import (
	"fmt"
	// "log"
	"time"

	// "github.com/gofunboost/boost"
	"github.com/gofunboost/boost"
	"github.com/gofunboost/broker"
	"github.com/gofunboost/core"
	// "go.uber.org/zap"
)

// 定义一个加法函数
func add(x, y int) int {
	result := x + y
	// log.Printf("计算 %d + %d = %d\n", x, y, result)
	boost.Sugar.Infof("计算 %d + %d = %d\n", x, y, result)
	time.Sleep(1 * time.Second)
	return result
}

// 定义一个打印函数
func printValue(a interface{}) {
	boost.Sugar.Infof("打印值: %v\n", a)
}

func main() {
	defer boost.Sugar.Sync()

	// 创建加法函数的Broker
	addOptions := core.BoostOptions{
		QueueName:     "queue_test2",
		ConsumeFunc:   add,
		BrokerKind:    core.REDIS,
		ConnNum:       5,
		ConcurrentNum: 50,
		QPSLimit:      2,
		MaxRetries:    3,
		BrokerConfig: core.Config{
			BrokerUrl: "localhost:6379",
			BrokerTransportOptions: map[string]interface{}{
				"special1": 123,
			},
		},
		Logger: boost.Sugar,
	}

	addBooster := broker.NewBroker(addOptions)

	// 创建打印函数的Broker
	printValueOptions := core.BoostOptions{
		QueueName:     "queue_test33",
		ConsumeFunc:   printValue,
		BrokerKind:    core.REDIS,
		ConnNum:       5,
		ConcurrentNum: 20,
		QPSLimit:      3,
		MaxRetries:    3,
		BrokerConfig: core.Config{
			BrokerUrl: "localhost:6379",
			BrokerTransportOptions: map[string]interface{}{
				"special1": 123,
				"special2": "aaaa",
			},
		},
		Logger: boost.Sugar,
	}

	printValueBooster := broker.NewBroker(printValueOptions)

	// 启动消费
	// addBooster.Consume()

	printValueBooster.Consume()

	// 推送消息
	for i := 0; i < 100; i++ {
		addBooster.Push(i, i*2)

		printValueBooster.Push(fmt.Sprintf("hello world %d", i))

		time.Sleep(500 * time.Millisecond)
	}

	// 阻塞进程，让所有协程池一直运行
	for {
		time.Sleep(10 * time.Second)
	}
}
