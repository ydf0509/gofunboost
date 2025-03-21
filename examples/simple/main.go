package main

import (
	"fmt"
	"time"
	"log"

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

		// 创建增强函数选项
		addOptions := boost.NewBoostOptions("queue_test",boost.WithQPSLimit(0.2))
	
		printOptions := boost.BoostOptions{
			QueueName:     "queue_test2",
			BrokerKind:    broker.REDIS,
			ConcurrentNum: 50,
			QPSLimit:      500,
			MaxRetries:    3,
			Timeout:       60,
			BrokerConfig: broker.Config{
				Address: "localhost:6379",
			},
		}

	// 创建增强函数
	addFunc := boost.Boost(*addOptions)(add)
	printFunc := boost.Boost(printOptions)(printValue)

	// 启动消费者
	addFunc.Consume()
	printFunc.Consume()

	// 推送任务到队列
	for i := 0; i < 100; i++ {
		// err := addFunc.Push(i, i*2)
		// if err != nil {
		// 	fmt.Printf("推送add任务失败: %v\n", err)
		// }

		// err = printFunc.Push(i)
		// if err != nil {
		// 	fmt.Printf("推送print任务失败: %v\n", err)
		// }
		addFunc.Push(i, i*2)
		printFunc.Push(i)
	}

	// 使用channel阻塞主程序，等待手动终止
	forever := make(chan struct{})
	<-forever
}