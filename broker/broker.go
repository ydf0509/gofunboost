package broker

import (
	// "context"
	"errors"
	"fmt"
	"time"

	"github.com/gofunboost/concurrentpool"
	"github.com/gofunboost/core"
	"go.uber.org/zap"
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

	impConsumeUsingOneConn() error
	// Push 推送消息到队列
	Push(args ...interface{})  (*core.Message, error)
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
	Pool         *concurrentpool.GoEasyPool
	StartTimeTs  int64
	limiter      *rate.Limiter
	imp          Broker

}

// NewBroker 创建一个新的Broker
func NewBroker(boostoptions core.BoostOptions) Broker {
	if boostoptions.QueueName == "" {
		panic("queue name is required")
	}

	if boostoptions.ConsumeFunc == nil {
		panic (ErrInvalidConsumeFunc)
	}

	b:= &BaseBroker{
		BoostOptions: boostoptions,
		Pool:         concurrentpool.NewGoEasyPool(boostoptions.ConcurrentNum),
		StartTimeTs:  time.Now().Unix(),
	}
	
	if b.QPSLimit > 0 {
		b.limiter = rate.NewLimiter(rate.Limit(b.QPSLimit), b.QPSLimit)
	}

	b.imp.newBrokerCustomInit()
	b.imp =b
	return b
}

func (b *BaseBroker) newBrokerCustomInit()  {
	
}


func (b *BaseBroker) Consume()  {
	imp := b.imp
	for i := 0; i < imp.ConnNum; i++ {
		go func() {
			imp.Logger.Info("Starting consumer connection", )
			imp.ConsumeUsingOneConn()
		}()
	}
}


func (b *BaseBroker) ConsumeUsingOneConn()  {
	imp := b.imp
	for {
		err:=imp.impConsumeUsingOneConn()
		if err != nil {
			imp.Logger.Error("Consumer connection error", zap.Error(err))
			time.Sleep(60 * time.Second)
		}
		
	}
}

func (b *BaseBroker) impConsumeUsingOneConn() error {
	// _imp := b.imp
	err :=errors.New("has no implementation")
	return err
}

func (b *BaseBroker) Push(args ...interface{}) (*core.Message, error) {
	imp:=b.imp
	msg := &core.Message{
		FucnArgs: args,
		PublishTs: time.Now().Unix(),
		PublishTimeStr: time.Now().Format("2006-01-02 15:04:05"),
		TaskId: fmt.Sprintf("%d", time.Now().UnixNano()),
	}
	return imp.Publish(msg)
}

func (b *BaseBroker) Publish(msg *core.Message) (*core.Message, error) {
	imp:=b.imp
	taskId := msg.TaskId
	if taskId == "" {
		taskId = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	msg.TaskId = taskId
	msg.PublishTs = time.Now().Unix()
	msg.PublishTimeStr = time.Now().Format("2006-01-02 15:04:05")

	msgStr := msg.ToJson()
	err := imp.impSendMsg(msgStr)
	return msg,err
}

func (b *BaseBroker) impSendMsg(msg string) error {
	imp:= b.imp
	return errors.New(fmt.Sprintf("has no implementation %s", imp))
}
