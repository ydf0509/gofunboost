package concurrentpool

import (
	// "fmt"
	"log"
	"sync"
	// "os"
	// "os/signal"
	// "syscall"
	"time"
)

// GoEasyPool 结构体
type GoEasyPool struct {
	maxWorkers int
	jobs       chan func() // 存储不同的工作函数
	once  sync.Once
}

// NewGoEasyPool 创建一个新的协程池
func NewGoEasyPool(maxWorkers int) *GoEasyPool {
	return &GoEasyPool{
		maxWorkers: maxWorkers,
		jobs:       make(chan func(), maxWorkers*2), // 工作队列，增大缓冲区
	}
}

// Run 启动协程池并处理工作
func (p *GoEasyPool) Run() {
	// 启动固定数量的工作协程
	for i := 0; i < p.maxWorkers; i++ {
		go func(workerId int) {
			log.Printf("工作协程 %d 已启动\n", workerId)
			for job := range p.jobs {
				job() // 执行工作
			}
		}(i)
	}
}

// Submit 提交一个工作到协程池
func (p *GoEasyPool) Submit(job func()) {
	p.once.Do(func(){p.Run()})
	p.jobs <- job
}

// // 示例函数1
// func work1b(id int, message string) {
// 	log.Printf("工作1 - 子协程 %d: %s \n", id, message)
// 	time.Sleep(5 * time.Second)
// }

// // 示例函数2
// func work2b(id int) {
// 	log.Printf("工作2 - 子协程 %d: \n", id)
// 	time.Sleep(4 * time.Second)
// }

// func main() {
// 	// 创建协程池，最大并发协程数为 3
// 	pool := NewGoEasyPool(3)

// 	// 启动协程池
// 	// pool.Run()

// 	// 提交不同的工作函数和参数
// 	for i := 1; i <= 5; i++ {
// 		id := i // 避免闭包问题

// 		// 直接定义一个匿名函数来调用 work1
// 		pool.Submit(func() {
// 			work1b(id, "Hello from work1")
// 		})

// 		// 直接定义一个匿名函数来调用 work2
// 		pool.Submit(func() {
// 			work2b(id)
// 		})
// 	}

// 	// select {}
// 	for {
// 		time.Sleep(1 * time.Second)
// 	}

// 	// // 使用信号通道保持程序运行
// 	// sigChan := make(chan os.Signal, 1)
// 	// signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
// 	// // 等待信号
// 	// <-sigChan
// 	// log.Println("收到信号，程序继续运行...")
	
// 	// // 程序不会退出，继续等待信号
// 	// <-sigChan
// 	// log.Println("再次收到信号，程序继续运行...")
	
// 	// // 无限等待信号
// 	// for {
// 	// 	<-sigChan
// 	// 	log.Println("收到信号，程序继续运行...")
// 	// }
// }