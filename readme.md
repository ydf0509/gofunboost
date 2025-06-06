
# 1. gofunboost 框架简介

gofunboost 是一个基于 go 语言的支持多种broker的消费框架，主要目的是自动实现多种消息队列的分布式消费，并施加控制功能，例如并发数 qps控频 重试等功能。
用户不需要再去关注怎么亲自操作消息队列。

但功能没用python funboost那么丰富。


# 3 使用例子

gofunboost 具体使用方式如下，当然没有python的 funboost装饰器一行那么简洁了，golang没有办法。

但是gofunboost比 go最知名的 machinery 消费框架用法简单，代码少一丝丝。

```golang
package main

import (
	"errors"
	"fmt"
	"math/rand"

	"log"
	"time"

	"github.com/ydf0509/gofunboost/broker"
	"github.com/ydf0509/gofunboost/core"
	"go.uber.org/zap/zapcore"
)

// core.Logger.SetLevel()
var logger, _, _ = core.InitLogger("gofunboost.log",zapcore.WarnLevel)

// 定义加法函数的参数结构体
type AddParams struct {
	X int
	Y int
}

// 定义一个加法函数
func add(params AddParams) (int, error) {
	result := params.X + params.Y
	log.Printf("计算 %d + %d = %d\n", params.X, params.Y, result)
	time.Sleep(1 * time.Second)
	if rand.Intn(100) > 95 {
		err := errors.New("for test add error")
		return 0, err
	}
	return result, nil
}

// 定义打印函数的参数结构体
type PrintParams struct {
	Value interface{}
}

// 定义一个打印函数
func printValue(params PrintParams) {
	log.Printf("打印值: %v\n", params.Value)
}

// var baseOptions = core.BoostOptions{
// 	BrokerKind:    core.MEMORY,
// 	ConnNum:       5,
// 	ConcurrentNum: 50,
// 	QPSLimit:      2,
// 	MaxRetries:    3,
// 	Logger:        core.Logger,
// 	BrokerConfig: core.Config{
// 		BrokerTransportOptions: map[string]interface{}{
// 			"memoryChanSize": 10000},
// 	},
// }

// var baseOptions = core.BoostOptions{
// 	BrokerKind:    core.SQLITE,
// 	ConnNum:       5,
// 	ConcurrentNum: 50,
// 	QPSLimit:      2,
// 	MaxRetries:    3,
// 	BrokerConfig: core.Config{
// 		BrokerTransportOptions: map[string]interface{}{
// 			"sqlite_dir": "/sqlite_queues",
// 		},
// 	},
// 	Logger: core.Logger,
// }

var baseOptions = core.BoostOptions{
	BrokerKind:    core.REDIS,
	ConnNum:       5,
	ConcurrentNum: 50,
	QPSLimit:      2,
	MaxRetries:    3,
	BrokerConfig: core.Config{
		BrokerUrl: "localhost:6379",
		BrokerTransportOptions: map[string]interface{}{
			"DB": 15,
		},
	},
	Logger: logger,
}

// var baseOptions = core.BoostOptions{
// 	BrokerKind:    core.RABBITMQ,
// 	ConnNum:       12,
// 	ConcurrentNum: 50,
// 	QPSLimit:      2,
// 	MaxRetries:    3,
// 	BrokerConfig: core.Config{
// 		BrokerUrl: "amqp://admin:123456abc@1.0.1.2:5672/",
// 		BrokerTransportOptions: map[string]interface{}{
// 			"special1": 123,
// 		},
// 	},
// 	Logger: core.Logger,
// }

// var baseOptions = core.BoostOptions{
// 	BrokerKind:    core.KAFKA,
// 	ConnNum:       5,
// 	ConcurrentNum: 50,
// 	QPSLimit:      2,
// 	MaxRetries:    3,
// 	BrokerConfig: core.Config{
// 		BrokerUrl: "192.0.1.1:9092",
// 		BrokerTransportOptions: map[string]interface{}{
// 			"groupId": "gofunboost",
// 		},
// 	},
// 	Logger: core.Logger,
// }

func main() {
	// defer core.Sugar.Sync()
	defer core.Logger.Sync()

	addBooster := broker.NewBroker(core.MergeBoostOptions(core.BoostOptions{
		QueueName:   "queue_test2",
		ConsumeFunc: add,
		QPSLimit:    -1,
	}, baseOptions))

	printValueBooster := broker.NewBroker(core.MergeBoostOptions(core.BoostOptions{
		QueueName:   "queue_test33",
		ConsumeFunc: printValue,
		QPSLimit:    0.5}, baseOptions))

	// 启动消费
	addBooster.Consume()
	printValueBooster.Consume()

	// 推送消息
	log.Println("start push")
	for i := 0; i < 100000; i++ {
		addBooster.Push(AddParams{
			X: i,
			Y: i * 2,
		})

		printValueBooster.Push(PrintParams{
			Value: fmt.Sprintf("hello world %d", i),
		})

		// time.Sleep(100 * time.Millisecond)
	}
	log.Println("push finish")
	fmt.Println("push finish")
	select {}

}

```