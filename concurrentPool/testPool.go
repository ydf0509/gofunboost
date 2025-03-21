package main

import (
	"fmt"
	"log"
	"time"
)

// 示例函数1
func work1(id int, message string) {

	log.Printf("工作1 - 子协程 %d: %s \n", id, message)
	time.Sleep(5 * time.Second)
}

// 示例函数2
func work2(id int) {

	log.Printf("工作2 - 子协程 %d: \n", id)
	time.Sleep(4 * time.Second)

}

func main() {
	// 创建协程池，最大并发协程数为 3
	pool := NewGoroutinePool(3)

	// 启动协程池
	pool.Run()

	// 提交不同的工作函数和参数
	for i := 1; i <= 5; i++ {
		id := i // 避免闭包问题

		// 直接定义一个匿名函数来调用 work1
		pool.Submit(func() {
			work1(id, "Hello from work1")
		})

		// 直接定义一个匿名函数来调用 work2
		pool.Submit(func() {
			work2(id)
		})
	}

	// 等待所有工作完成``
	pool.Wait()
	// select {}
	fmt.Println("主协程结束")
}
