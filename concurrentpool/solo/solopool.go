package solo

/**
direct run a function ,not concurret ,but implement PoolSubmit interface

*/

import (
	"github.com/ydf0509/gofunboost/concurrentpool"
)

// GoEasyPool 结构体
type GoPool struct {
}

func (p *GoPool) Submit(task func()) {
	task()
}

func NewGoPool(maxWorkers int) concurrentpool.PoolSubmit {
	pool := &GoPool{}
	return pool
}
