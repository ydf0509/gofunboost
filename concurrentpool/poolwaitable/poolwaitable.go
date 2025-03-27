package poolwaitable

import (
	"sync"
	"github.com/ydf0509/gofunboost/concurrentpool"
)

// GoPool 结构体
type GoPool struct {
	maxWorkers int
	jobs       chan func()    // 存储不同的工作函数
	wg         sync.WaitGroup // 添加 WaitGroup 来跟踪工作完成情况
}

// NewGoPool 创建一个新的协程池
func NewGoPool(maxWorkers int) concurrentpool.PoolSubmit {
	return &GoPool{
		maxWorkers: maxWorkers,
		jobs:       make(chan func(), maxWorkers*1), // 工作队列
	}
}

// Run 启动协程池并处理工作
func (p *GoPool) Run() {
	// 启动固定数量的工作协程
	for i := 0; i < p.maxWorkers; i++ {
		go func() {
			for job := range p.jobs {
				job()       // 执行工作
				p.wg.Done() // 标记一个工作完成
			}
		}()
	}
}

// Submit 提交一个工作到协程池
func (p *GoPool) Submit(job func()) {
	p.wg.Add(1) // 添加一个工作到等待组
	p.jobs <- job
}

// Wait 等待所有工作完成
func (p *GoPool) Wait() {
	p.wg.Wait()   // 等待所有工作完成
	close(p.jobs) // 关闭工作队列
}
