

package concurrentpool

type PoolSubmit interface {
	Submit(job func())
}