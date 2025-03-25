package main

import (
	"errors"
	"fmt"
	// "log"
	"time"

	// "github.com/gofunboost/boost"
	"github.com/gofunboost/boost"
	"github.com/gofunboost/broker"
	"github.com/gofunboost/core"
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
	boost.Sugar.Infof("计算 %d + %d = %d\n", params.X, params.Y, result)
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
	boost.Sugar.Infof("打印值: %v\n", params.Value)
}

var baseOptions = core.BoostOptions{
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
	Logger: boost.Logger,
}

func main() {
	defer boost.Sugar.Sync()

	addOptions := core.MergeBoostOptions(core.BoostOptions{
		QueueName:   "queue_test2",
		ConsumeFunc: add,
	}, &baseOptions)

	addBooster := broker.NewBroker(addOptions)

	// 创建打印函数的Broker
	printValueOptions := core.MergeBoostOptions(core.BoostOptions{
		QueueName:   "queue_test33",
		ConsumeFunc: printValue,
		QPSLimit:    0.2}, &baseOptions)

	printValueBooster := broker.NewBroker(printValueOptions)

	// 启动消费
	addBooster.Consume()

	printValueBooster.Consume()

	// 推送消息
	for i := 0; i < 100; i++ {
		addParams := AddParams{
			X: i,
			Y: i * 2,
		}
		addBooster.Push(addParams)

		printParams := PrintParams{
			Value: fmt.Sprintf("hello world %d", i),
		}
		printValueBooster.Push(printParams)

		time.Sleep(100 * time.Millisecond)
	}

	// 阻塞进程，让所有协程池一直运行
	for {
		time.Sleep(10 * time.Second)
	}
}
