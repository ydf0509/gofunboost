package broker

import (
	"context"
	"fmt"

	"github.com/ydf0509/gofunboost/core"
)

// MemoryBroker 实现基于内存的消息队列
type MemoryBroker struct {
	*BaseBroker
	msgChan chan *core.Message
}

func (b *MemoryBroker) newBrokerCustomInit() {
	b.Sugar.Infof("newMemoryBrokerCustomInit %v", b)
	// 创建带缓冲的消息通道
	if b.BrokerConfig.BrokerTransportOptions["memoryChanSize"] == nil {
		b.BrokerConfig.BrokerTransportOptions["memoryChanSize"] = 10000
	}
	b.msgChan = make(chan *core.Message, b.BrokerConfig.BrokerTransportOptions["memoryChanSize"].(int))

}

// ConsumeUsingOneConn 使用一个连接消费消息
func (b *MemoryBroker) impConsumeUsingOneConn() error {
	b.Logger.Info("Memory broker started consuming messages")

	ctx := context.Background()
	// 持续消费消息
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-b.msgChan:
			// 使用协程池处理消息
			b.Pool.Submit(func() { b.execute(&core.MessageWrapper{Msg: msg}) })
		}
	}
}

func (b *MemoryBroker) impSendMsg(msg string) error {
	// 将消息字符串转换为Message对象
	message := b.imp.Json2Message(msg)
	if message == nil {
		err := fmt.Errorf("failed to parse message: %s", msg)
		err2 := core.NewFunboostRunError("Failed to parse message", 0, err, b.Logger)
		err2.Log()
		return err2
	}

	// 使用阻塞方式发送消息
	b.msgChan <- message
	return nil
}

func (b *MemoryBroker) impAckMsg(msg *core.MessageWrapper) error {
	// 内存队列不需要显式的ACK
	return nil
}
