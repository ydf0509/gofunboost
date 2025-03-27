package simplepool

import (
	"github.com/ydf0509/gofunboost/concurrentpool"
)

// GoEasyPool 结构体
type GoPool struct {
	maxWorkers int
	spool      chan struct{}
}

// NewGoEasyPool 创建一个新的协程池
func NewGoPool(maxWorkers int) concurrentpool.PoolSubmit {
	pool := &GoPool{
		maxWorkers: maxWorkers,
	}
	spool := make(chan struct{}, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		spool <- struct{}{}
	}
	pool.spool = spool
	return pool
}

func (p *GoPool) Submit(job func()) {
	<-p.spool
	go func() {
		defer func() {
			p.spool <- struct{}{}
		}()
		job()
	}()
}
