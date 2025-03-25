package broker

import (
	// "context"
	"errors"
	"fmt"
	"time"

	"github.com/gofunboost/concurrentpool"
	"github.com/gofunboost/core"
	"go.uber.org/zap"

	// "go.uber.org/zap"
	"golang.org/x/time/rate"
	// "go.uber.org/zap"
)

// 定义错误
var (
	ErrUnsupportedBrokerType = errors.New("unsupported broker type")
	ErrInvalidConsumeFunc    = errors.New("invalid consume function")
	ErrRedisConnection       = errors.New("redis connection error")
)

// BaseBroker 定义基础Broker接口
type Broker interface {
	// Consume 启动消费函数
	Consume()
	// ConsumeUsingOneConn 使用一个连接消费消息
	// ConsumeUsingOneConn(ctx context.Context) error
	ConsumeUsingOneConn()
	impConsumeUsingOneConn() error
	// Push 推送消息到队列
	Push(args ...interface{}) (*core.Message, error)
	Publish(msg *core.Message) (*core.Message, error)

	impSendMsg(msg string) error

	impAckMsg(msg *core.MessageWrapper) error

	newBrokerCustomInit()

	// Close 关闭Broker
	// Close() error
}

// Broker 定义Broker结构体
type BaseBroker struct {
	core.BoostOptions
	Pool        *concurrentpool.GoEasyPool
	StartTimeTs int64
	limiter     *rate.Limiter
	imp         Broker
}

// NewBroker 创建一个新的Broker
func NewBroker(boostoptions core.BoostOptions) Broker {
	if boostoptions.QueueName == "" {
		panic("queue name is required")
	}

	if boostoptions.ConsumeFunc == nil {
		panic(ErrInvalidConsumeFunc)
	}

	base := &BaseBroker{
		BoostOptions: boostoptions,
		Pool:         concurrentpool.NewGoEasyPool(boostoptions.ConcurrentNum),
		StartTimeTs:  time.Now().Unix(),
	}

	if base.QPSLimit > 0 {
		base.limiter = rate.NewLimiter(rate.Limit(base.QPSLimit), 1)
	}

	var broker Broker
	switch boostoptions.BrokerKind {
	case core.REDIS:
		redisBroker := &RedisBroker{BaseBroker: base}
		redisBroker.imp = redisBroker
		broker = redisBroker
	default:
		base.imp = base
		broker = base
	}

	broker.newBrokerCustomInit()
	return broker
}

func (b *BaseBroker) newBrokerCustomInit() {
	b.Logger.Infof("newBrokerCustomInit %v", b)
}

func (b *BaseBroker) Consume() {
	for i := 0; i < b.ConnNum; i++ {
		go func() {
			b.Logger.Info("Starting consumer connection")
			b.imp.ConsumeUsingOneConn()
		}()
	}
}

func (b *BaseBroker) ConsumeUsingOneConn() {
	for {
		err := b.imp.impConsumeUsingOneConn()
		if err != nil {
			b.Logger.Error("Consumer connection error", zap.Error(err))
			time.Sleep(60 * time.Second)
		}

	}
}

func (b *BaseBroker) impConsumeUsingOneConn() error {
	// _imp := b.imp
	err := errors.New("has no implementation")
	return err
}

func (b *BaseBroker) Push(args ...interface{}) (*core.Message, error) {
	msg := &core.Message{
		FucnArgs:       args,
		PublishTs:      time.Now().Unix(),
		PublishTimeStr: time.Now().Format("2006-01-02 15:04:05"),
		TaskId:         fmt.Sprintf("%d", time.Now().UnixNano()),
	}
	// fmt.Printf("%v  %v  %v", b.QueueName, b, b.imp)
	// b.Logger.Infof("%v  %v", b, b.imp)

	return b.imp.Publish(msg)
}

func (b *BaseBroker) Publish(msg *core.Message) (*core.Message, error) {
	taskId := msg.TaskId
	if taskId == "" {
		taskId = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	msg.TaskId = taskId
	msg.PublishTs = time.Now().Unix()
	msg.PublishTimeStr = time.Now().Format("2006-01-02 15:04:05")

	msgStr := msg.ToJson()
	err := b.imp.impSendMsg(msgStr)
	return msg, err
}

func (b *BaseBroker) impSendMsg(msg string) error {
	return errors.New(fmt.Sprintf("impSendMsg has no implementation %s", b.imp))
}

func (b *BaseBroker) impAckMsg(msg *core.MessageWrapper) error {
	return errors.New(fmt.Sprintf(" impAckMsg has no implementation %s", b.imp))
}
