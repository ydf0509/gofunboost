package boost

import (
	"context"
	"errors"
	"fmt"

	"sync"
	"time"

	"github.com/gofunboost/broker"
	"github.com/gofunboost/task"
	"golang.org/x/time/rate"
)

// BrokerEnum 消息队列类型枚举
// type BrokerEnum = broker.BrokerKind

// OptionFunc 定义选项配置函数类型
// type OptionFunc func(*BoostOptions)




// BoostOptions 函数增强选项
type BoostOptions struct {
	// 队列名称
	QueueName string

	// 消息队列类型
	BrokerKind string 

	// 并发数量
	ConcurrentNum int 

	// QPS限制
	QPSLimit float64 

	// 最大重试次数
	MaxRetries int 

	// 函数执行超时时间（秒）
	Timeout int 

	// 消息队列配置
	BrokerConfig broker.Config 
}

type BoostOptionsOpts func(*BoostOptions)

func NewBoostOptions(queueName string,opts ...BoostOptionsOpts) *BoostOptions {
	bo :=&BoostOptions{
		QueueName:queueName,
		BrokerKind:    broker.REDIS,
		ConcurrentNum: 50,
		QPSLimit:      500,
		MaxRetries:    3,
		Timeout:       60,
		BrokerConfig: broker.Config{
			Address: "localhost:6379",
		},
	}
	for _, opt := range opts {
        opt(bo)
    }
	return bo
}

func WithBrokerKind(brokerKind string) BoostOptionsOpts {
    return func(bo *BoostOptions) {
        bo.BrokerKind = brokerKind
    }
}

func WithConcurrentNum(concurrentNum int) BoostOptionsOpts {
    return func(bo *BoostOptions) {
        bo.ConcurrentNum = concurrentNum
    }
}

func WithQPSLimit(qpsLimit float64) BoostOptionsOpts {
    return func(bo *BoostOptions) {
        bo.QPSLimit = qpsLimit
    }
}

func WithMaxRetries(maxRetries int) BoostOptionsOpts {
    return func(bo *BoostOptions) {
        bo.MaxRetries = maxRetries
    }
}

func WithTimeout(timeout int) BoostOptionsOpts {
    return func(bo *BoostOptions) {
        bo.Timeout = timeout
    }
}

func WithBrokerAddress(address string) BoostOptionsOpts {
    return func(bo *BoostOptions) {
        bo.BrokerConfig.Address = address
    }
}


// FunctionRegistry 函数注册表
var functionRegistry = make(map[string]interface{})

// BoostFunc 增强函数结构体
type BoostFunc struct {
	// 函数名称
	name string

	// 原始函数
	fn interface{}

	// 选项
	options BoostOptions

	// 消息队列
	broker broker.Broker

	// 速率限制器
	limiter *rate.Limiter

	// 工作协程池
	workers []*Worker

	// 上下文和取消函数
	ctx    context.Context
	cancel context.CancelFunc

	// 互斥锁
	mutex sync.Mutex

	// 是否正在消费
	consuming bool
}

// Worker 工作协程
type Worker struct {
	id      int
	boostFn *BoostFunc
	ctx     context.Context
	wg      *sync.WaitGroup
}

// Boost 创建一个增强函数
func Boost(options BoostOptions) func(interface{}) *BoostFunc {
	return func(fn interface{}) *BoostFunc {
		// 获取函数名称
		fnName := getFunctionName(fn)

		// 设置默认值
		if options.ConcurrentNum <= 0 {
			options.ConcurrentNum = 1
		}
		if options.MaxRetries <= 0 {
			options.MaxRetries = 3
		}
		if options.Timeout <= 0 {
			options.Timeout = 60
		}

		// 设置消息队列配置
		options.BrokerConfig.Kind = options.BrokerKind

		// 创建速率限制器
		var limiter *rate.Limiter
		if options.QPSLimit > 0 {
			// 当QPSLimit小于1时，需要特殊处理rate和burst参数
			// 例如：QPSLimit=0.2表示每5秒执行一次
			if options.QPSLimit < 1 {
				limiter = rate.NewLimiter(rate.Limit(1), 1)
				limiter.SetLimit(rate.Limit(options.QPSLimit))
			} else {
				burst := int(options.QPSLimit)
				limiter = rate.NewLimiter(rate.Limit(options.QPSLimit), burst)
			}
		}

		// 创建上下文
		ctx, cancel := context.WithCancel(context.Background())

		// 注册函数
		functionRegistry[fnName] = fn

		return &BoostFunc{
			name:    fnName,
			fn:      fn,
			options: options,
			limiter: limiter,
			ctx:     ctx,
			cancel:  cancel,
		}
	}
}

// Push 将任务推送到队列
func (bf *BoostFunc) Push(args ...interface{}) error {
	// 初始化消息队列（如果尚未初始化）
	if err := bf.initBroker(); err != nil {
		return err
	}

	// 创建任务
	t := task.NewTask(bf.name, args, bf.options.MaxRetries)

	// 序列化任务
	data, err := t.Serialize()
	if err != nil {
		return err
	}

	// 推送到队列
	return bf.broker.Push(bf.options.QueueName, data)
}

// Consume 开始消费队列中的任务
func (bf *BoostFunc) Consume() error {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()

	// 检查是否已经在消费
	if bf.consuming {
		return errors.New("已经在消费队列")
	}

	// 创建工作协程池
	bf.workers = make([]*Worker, bf.options.ConcurrentNum)
	wg := &sync.WaitGroup{}

	// 启动自动重连和消费协程
	go func() {
		for {
			select {
			case <-bf.ctx.Done():
				return
			default:
				// 初始化消息队列
				if err := bf.initBroker(); err != nil {
					fmt.Printf("连接消息队列失败: %v，60秒后重试\n", err)
					time.Sleep(60 * time.Second)
					continue
				}

				// 启动工作协程
				for i := 0; i < bf.options.ConcurrentNum; i++ {
					wg.Add(1)
					worker := &Worker{
						id:      i,
						boostFn: bf,
						ctx:     bf.ctx,
						wg:      wg,
					}
					bf.workers[i] = worker
					go worker.start()
				}

				// 标记为正在消费
				bf.consuming = true

				// 等待上下文取消
				<-bf.ctx.Done()

				// 等待工作协程退出
				for _, worker := range bf.workers {
					worker.wg.Wait()
				}

				// 关闭消息队列连接
				if bf.broker != nil {
					bf.broker.Close()
					bf.broker = nil
				}

				// 重置状态
				bf.consuming = false
				return
			}
		}
	}()

	return nil
}

// Stop 停止消费
func (bf *BoostFunc) Stop() {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()

	if !bf.consuming {
		return
	}

	// 取消上下文
	bf.cancel()

	// 等待所有工作协程退出
	for _, worker := range bf.workers {
		worker.wg.Wait()
	}

	// 关闭消息队列连接
	if bf.broker != nil {
		bf.broker.Close()
		bf.broker = nil
	}

	// 重置上下文
	bf.ctx, bf.cancel = context.WithCancel(context.Background())

	// 标记为未消费
	bf.consuming = false
}

// 初始化消息队列
func (bf *BoostFunc) initBroker() error {
	if bf.broker != nil {
		return nil
	}

	// 创建消息队列实例
	b, err := broker.NewBroker(bf.options.BrokerConfig)
	if err != nil {
		return err
	}

	bf.broker = b
	return nil
}

// 工作协程启动函数
func (w *Worker) start() {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			// 上下文被取消，退出
			return
		default:
			// 处理任务
			w.processTask()
		}
	}
}

// 处理任务
func (w *Worker) processTask() {
	// 获取消息队列和选项
	bf := w.boostFn
	broker := bf.broker
	options := bf.options

	// 从队列中获取任务
	data, err := broker.Pop(options.QueueName)
	if err != nil || data == nil {
		// 队列为空或出错，等待一段时间再试
		time.Sleep(100 * time.Millisecond)
		return
	}

	// 反序列化任务
	t, err := task.Deserialize(data)
	if err != nil {
		// 任务反序列化失败，忽略
		return
	}

	// 获取注册的函数
	fn, ok := functionRegistry[t.FuncName]
	if !ok {
		// 函数未注册，忽略
		return
	}

	// 应用QPS限制
	if bf.limiter != nil {
		if err := bf.limiter.Wait(w.ctx); err != nil {
			// 上下文被取消，退出
			return
		}
	}

	// 创建超时上下文
	timeoutCtx, cancel := context.WithTimeout(w.ctx, time.Duration(options.Timeout)*time.Second)
	defer cancel()

	// 创建结果通道
	resultCh := make(chan struct {
		result interface{}
		err    error
	}, 1)

	// 在协程中执行函数
	go func() {
		result, err := task.CallFunction(fn, t.Args)
		resultCh <- struct {
			result interface{}
			err    error
		}{result, err}
	}()

	// 等待函数执行完成或超时
	select {
	case <-timeoutCtx.Done():
		// 函数执行超时
		if timeoutCtx.Err() == context.DeadlineExceeded {
			// 如果可以重试，则重新入队
			if t.CanRetry() {
				t.IncrementRetry()
				data, _ := t.Serialize()
				broker.Push(options.QueueName, data)
			}
		}
	case res := <-resultCh:
		// 函数执行完成
		if res.err != nil {
			// 执行失败，如果可以重试，则重新入队
			if t.CanRetry() {
				t.IncrementRetry()
				data, _ := t.Serialize()
				broker.Push(options.QueueName, data)
			}
		}
	}
}

// 获取函数名称
func getFunctionName(fn interface{}) string {
	return fmt.Sprintf("%T", fn)
}