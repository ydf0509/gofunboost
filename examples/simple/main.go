package main

import (
	"errors"
	"fmt"

	// "log"
	"time"

	"github.com/ydf0509/gofunboost/broker"
	"github.com/ydf0509/gofunboost/core"
	// "go.uber.org/zap"
)

// 定义加法函数的参数结构体
type AddParams struct {
	X int
	Y int
}

// 定义一个加法函数
func add(params AddParams) (int, error) {
	result := params.X + params.Y
	core.Sugar.Infof("计算 %d + %d = %d\n", params.X, params.Y, result)
	time.Sleep(1 * time.Second)
	if params.X == 10 {
		err := errors.New("test error")
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
	core.Sugar.Infof("打印值: %v\n", params.Value)
}

var baseOptions = core.BoostOptions{
	BrokerKind:    core.MEMORY,
	ConnNum:       5,
	ConcurrentNum: 50,
	QPSLimit:      2,
	MaxRetries:    3,
	Logger:        core.Logger,
}

// var baseOptions = core.BoostOptions{
// 	BrokerKind:    core.REDIS,
// 	ConnNum:       5,
// 	ConcurrentNum: 50,
// 	QPSLimit:      2,
// 	MaxRetries:    3,
// 	BrokerConfig: core.Config{
// 		BrokerUrl: "localhost:6379",
// 		BrokerTransportOptions: map[string]interface{}{
// 			"special1": 123,
// 		},
// 	},
// 	Logger: core.Logger,
// }

// var baseOptions = core.BoostOptions{
// 	BrokerKind:    core.RABBITMQ,
// 	ConnNum:       12,
// 	ConcurrentNum: 50,
// 	QPSLimit:      2,
// 	MaxRetries:    3,
// 	BrokerConfig: core.Config{
// 		BrokerUrl: "amqp://admin:372148@106.55.244.110:5672/",
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
// 		BrokerUrl: "192.168.1.105:9092",
// 		BrokerTransportOptions: map[string]interface{}{
// 			"groupId": "gofunboost",
// 		},
// 	},
// 	Logger: core.Logger,
// }

func main() {
	defer core.Sugar.Sync()
	defer core.Logger.Sync()

	addOptions := core.MergeBoostOptions(core.BoostOptions{
		QueueName:   "queue_test2",
		ConsumeFunc: add,
	}, baseOptions)

	addBooster := broker.NewBroker(addOptions)

	// 创建打印函数的Broker
	printValueOptions := core.MergeBoostOptions(core.BoostOptions{
		QueueName:   "queue_test33",
		ConsumeFunc: printValue,
		QPSLimit:    0.5}, baseOptions)

	printValueBooster := broker.NewBroker(printValueOptions)

	// 启动消费
	addBooster.Consume()

	printValueBooster.Consume()

	// 推送消息
	for i := 0; i < 100; i++ {
		// addParams := AddParams{
		// 	X: i,
		// 	Y: i * 2,
		// }
		// addBooster.Push(addParams)

		printParams := PrintParams{
			Value: fmt.Sprintf("hello world %d", i),
		}
		printValueBooster.Push(printParams)

		// time.Sleep(100 * time.Millisecond)
	}

	// // 阻塞进程，让所有协程池一直运行
	// for {
	// 	time.Sleep(10 * time.Second)
	// }

	select {}

	fmt.Println("aaaa")
}
