package broker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// MemoryMessage 定义内存消息结构
type MemoryMessage struct {
	ID   string        `json:"id"`
	Args []interface{} `json:"args"`
}

// MemoryQueue 定义内存队列
type MemoryQueue struct {
	mutex    sync.RWMutex
	messages []MemoryMessage
}

// NewMemoryQueue 创建内存队列
func NewMemoryQueue() *MemoryQueue {
	return &MemoryQueue{
		messages: make([]MemoryMessage, 0),
	}
}

// Push 推送消息到队列
func (q *MemoryQueue) Push(message MemoryMessage) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.messages = append(q.messages, message)
}

// Pop 从队列中获取消息
func (q *MemoryQueue) Pop() (MemoryMessage, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if len(q.messages) == 0 {
		return MemoryMessage{}, false
	}

	message := q.messages[0]
	q.messages = q.messages[1:]
	return message, true
}

// MemoryAcker 实现Acker接口
type MemoryAcker struct {
	Queue *MemoryQueue
	Msg   MemoryMessage
}

// Ack 确认消息已处理
func (a *MemoryAcker) Ack() error {
	// 内存队列不需要显式确认，消息已经从队列中移除
	return nil
}

// Nack 拒绝消息
func (a *MemoryAcker) Nack() error {
	// 将消息重新放回队列
	a.Queue.Push(a.Msg)
	return nil
}

// 全局内存队列映射
var (
	memoryQueues     = make(map[string]*MemoryQueue)
	memoryQueueMutex sync.RWMutex
)

// getOrCreateQueue 获取或创建内存队列
func getOrCreateQueue(name string) *MemoryQueue {
	memoryQueueMutex.Lock()
	defer memoryQueueMutex.Unlock()

	if queue, ok := memoryQueues[name]; ok {
		return queue
	}

	queue := NewMemoryQueue()
	memoryQueues[name] = queue
	return queue
}

// MemoryBroker 实现内存消息队列
type MemoryBroker struct {
	*Broker
	queue   *MemoryQueue
	limiter *rate.Limiter
}

// NewMemoryBroker 创建内存Broker
func NewMemoryBroker(broker *Broker) (BaseBroker, error) {
	if broker.QueueName == "" {
		return nil, errors.New("queue name is required")
	}

	if broker.ConsumeFunc == nil {
		return nil, ErrInvalidConsumeFunc
	}

	// 创建QPS限制器
	var limiter *rate.Limiter
	if broker.QPSLimit > 0 {
		limiter = rate.NewLimiter(rate.Limit(broker.QPSLimit), broker.QPSLimit)
	}

	// 获取或创建队列
	queue := getOrCreateQueue(broker.QueueName)

	return &MemoryBroker{
		Broker:  broker,
		queue:   queue,
		limiter: limiter,
	}, nil
}

// Consume 实现消费逻辑
func (m *MemoryBroker) Consume() error {
	m.Logger.Info("Starting Memory consumer",
		zap.String("queue", m.QueueName),
		zap.Int("conn_num", m.ConnNum),
		zap.Int("concurrent_num", m.ConcurrentNum))

	// 创建多个连接进行消费
	for i := 0; i < m.ConnNum; i++ {
		go func(connID int) {
			m.Logger.Info("Starting consumer connection", zap.Int("conn_id", connID))
			for {
				ctx := context.Background()
				err := m.ConsumeUsingOneConn(ctx)
				if err != nil {
					m.Logger.Error("Consumer connection failed",
						zap.Int("conn_id", connID),
						zap.Error(err))
					// 等待一段时间后重试
					time.Sleep(60 * time.Second)
				}
			}
		}(i)
	}

	return nil
}

// ConsumeUsingOneConn 使用一个连接消费消息
func (m *MemoryBroker) ConsumeUsingOneConn(ctx context.Context) error {
	m.Logger.Info("Memory connection established")

	// 持续消费消息
	for {
		// 检查上下文是否已取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// 继续执行
		}

		// 如果设置了QPS限制，则等待令牌
		if m.limiter != nil {
			m.limiter.Wait(ctx)
		}

		// 从队列中获取消息
		message, ok := m.queue.Pop()
		if !ok {
			// 队列为空，等待一段时间后重试
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// 创建Acker
		acker := &MemoryAcker{
			Queue: m.queue,
			Msg:   message,
		}

		// 提交任务到协程池
		m.Pool.Submit(func() {
			m.processMessage(message.Args, acker)
		})
	}
}

// processMessage 处理消息
func (m *MemoryBroker) processMessage(args []interface{}, acker Acker) {
	// 获取函数的反射值
	funcValue := reflect.ValueOf(m.ConsumeFunc)
	funcType := funcValue.Type()

	// 检查参数数量是否匹配
	if len(args) != funcType.NumIn() {
		m.Logger.Error("Parameter count mismatch",
			zap.Int("expected", funcType.NumIn()),
			zap.Int("got", len(args)))
		return
	}

	// 准备参数
	in := make([]reflect.Value, len(args))
	for i, arg := range args {
		// 获取期望的参数类型
		expectedType := funcType.In(i)

		// 获取实际参数的值和类型
		argValue := reflect.ValueOf(arg)
		argType := argValue.Type()

		// 检查类型是否兼容
		if !argType.AssignableTo(expectedType) {
			// 尝试类型转换
			if argValue.CanConvert(expectedType) {
				argValue = argValue.Convert(expectedType)
			} else {
				m.Logger.Error("Parameter type mismatch",
					zap.Int("parameter", i),
					zap.String("expected", expectedType.String()),
					zap.String("got", argType.String()))
				return
			}
		}

		in[i] = argValue
	}

	// 调用函数
	results := funcValue.Call(in)

	// 确认消息已处理
	if acker != nil {
		if err := acker.Ack(); err != nil {
			m.Logger.Error("Failed to ack message", zap.Error(err))
		}
	}

	// 处理返回值
	if len(results) > 0 {
		result := results[0].Interface()
		m.Logger.Info("Task executed successfully",
			zap.Any("result", result),
			zap.Any("args", args))
	} else {
		m.Logger.Info("Task executed successfully with no result",
			zap.Any("args", args))
	}
}

// Push 推送消息到队列
func (m *MemoryBroker) Push(args ...interface{}) error {
	// 创建消息
	message := MemoryMessage{
		ID:   fmt.Sprintf("%s:%d", m.QueueName, time.Now().UnixNano()),
		Args: args,
	}

	// 推送消息到队列
	m.queue.Push(message)

	return nil
}

// Close 关闭Broker
func (m *MemoryBroker) Close() error {
	// 内存队列不需要关闭
	return nil
}
