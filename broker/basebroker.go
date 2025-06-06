package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/ydf0509/gofunboost/concurrentpool"
	"github.com/ydf0509/gofunboost/core"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// BaseBroker 定义基础Broker接口
type Broker interface {
	// Consume 启动消费函数
	Consume()
	// ConsumeUsingOneConn 使用一个连接消费消息
	// ConsumeUsingOneConn(ctx context.Context) error
	ConsumeUsingOneConn()
	impConsumeUsingOneConn() error
	// Push 推送消息到队列，参数必须是一个结构体
	Push(data interface{}) (*core.Message, error)
	Publish(msg *core.Message) (*core.Message, error)

	impSendMsg(msg string) error

	impAckMsg(msg *core.MessageWrapper) error

	newBrokerCustomInit()

	execute(messageWrapper *core.MessageWrapper)

	Json2Message(msgStr string) *core.Message

	Clear() error

	// Close 关闭Broker
	// Close() error
}

// Broker 定义Broker结构体
type BaseBroker struct {
	core.BoostOptions
	Pool        concurrentpool.PoolSubmit
	StartTimeTs int64
	limiter     *rate.Limiter
	FuncValue   reflect.Value
	FuncType    reflect.Type
	ParamType   reflect.Type
	// ParamValue  reflect.Value
	Sugar *zap.SugaredLogger
	imp   Broker
}

func (b *BaseBroker) newBrokerCustomInit() {
	b.Sugar.Infof("newBrokerCustomInit %v", b)
}

func (b *BaseBroker) Consume() {
	for i := 0; i < b.ConnNum; i++ {
		go func() {
			b.Sugar.Info("Starting consumer connection")
			b.imp.ConsumeUsingOneConn()
		}()
	}
}

func (b *BaseBroker) ConsumeUsingOneConn() {
	for {
		err := b.imp.impConsumeUsingOneConn()
		if err != nil {
			// errType := err.(type)
			// if errx, ok := interface{}(err).(core.BrokerNetworkError); ok
			if errx, ok := err.(*core.BrokerNetworkError); ok {
				b.Sugar.Error("Consumer connection error", zap.Error(errx))
				time.Sleep(60 * time.Second)
			}
			b.Sugar.Error("not conn error", zap.Error(err))
		}
		b.Sugar.Error("impConsumeUsingOneConn has exit ")
		time.Sleep(1 * time.Second)
	}
}

func (b *BaseBroker) impConsumeUsingOneConn() error {
	// _imp := b.imp
	err := core.NewFunboostRunError("impConsumeUsingOneConn has no implementation", 0, nil, b.Logger)
	err.Log()
	panic(err)
}

func (b *BaseBroker) Push(data interface{}) (*core.Message, error) {
	msg := &core.Message{
		Data: data,
	}
	// fmt.Printf("%v  %v  %v", b.QueueName, b, b.imp)
	// b.Logger.Infof("%v  %v", b, b.imp)

	return b.imp.Publish(msg)
}

func GenTaskid() string {
	return fmt.Sprintf("%013d_%07d", time.Now().UnixNano()/1e6, rand.Intn(10000000))
}

func (b *BaseBroker) Clear() error {
	return core.NewFunboostRunError(fmt.Sprintf("Clear has no implementation %s", b.imp), 0, nil, b.Logger)
}

func (b *BaseBroker) Publish(msg *core.Message) (*core.Message, error) {
	taskId := msg.TaskId
	if taskId == "" {
		taskId = GenTaskid()
	}
	msg.TaskId = taskId
	msg.PublishTs = time.Now().Unix()
	msg.PublishTimeStr = time.Now().Format("2006-01-02 15:04:05")

	msgStr := msg.ToJson()
	err := b.imp.impSendMsg(msgStr)
	b.Logger.Info("Publish msg ", zap.String("msg", msgStr), zap.Error(err))
	return msg, err
}

func (b *BaseBroker) impSendMsg(msg string) error {
	err := core.NewBrokerNetworkError(fmt.Sprintf("impSendMsg has no implementation %s", b.imp), 0, nil, b.Logger)
	err.Log()
	panic(err)
}

func (b *BaseBroker) impAckMsg(msg *core.MessageWrapper) error {
	err := core.NewBrokerNetworkError(fmt.Sprintf("impAckMsg has no implementation %s", b.imp), 0, nil, b.Logger)
	err.Log()
	panic(err)
}

func (b *BaseBroker) Json2Message(msgStr string) *core.Message {
	var msg core.Message
	err := json.Unmarshal([]byte(msgStr), &msg)
	if err != nil {
		return nil
	}

	// 将msg.Data转换为JSON字符串
	jsonData, err := json.Marshal(msg.Data)
	if err != nil {
		err2 := core.NewFunboostRunError(fmt.Sprintf("Failed to marshal message data: %v", err), 0, err, b.Logger)
		err2.Log()
		return nil
	}

	// 将JSON字符串反序列化为目标类型
	paramValue := reflect.New(b.ParamType).Interface()
	if err := json.Unmarshal(jsonData, paramValue); err != nil {
		err2 := core.NewFunboostRunError(fmt.Sprintf("Failed to unmarshal message data: %v", err), 0, err, b.Logger)
		err2.Log()
		return nil
	}

	msg.Data = paramValue
	// msg.Data = reflect.ValueOf(paramValue).Elem()
	return &msg
}

func (b *BaseBroker) execute(messageWrapper *core.MessageWrapper) {
	// 如果设置了QPS限制，则等待令牌
	if b.limiter != nil {
		b.limiter.Wait(context.Background())
	}

	msg := messageWrapper.Msg
	isFunRunError := false
	for rt := 0; rt < b.MaxRetries; rt++ {
		results := b.FuncValue.Call([]reflect.Value{reflect.ValueOf(msg.Data).Elem()})
	
		// 检查是否有错误返回
		if len(results) > 0 {
			lastResult := results[len(results)-1]
			if lastResult.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) && !lastResult.IsNil() {
				err := results[len(results)-1].Interface().(error)
				if err != nil {
					err2 := core.NewFunboostRunError(fmt.Sprintf("Failed to process message (TaskId: %s): %v, Data: %+v , currentRetryTimes : %v", msg.TaskId, err, msg.Data, rt), 0, err, b.Logger)
					err2.Log()
					isFunRunError = true
				} else {
					isFunRunError = false
				}
			}
		}
		if isFunRunError == false {
			break
		}

	}
	if isFunRunError == false {
		b.Sugar.Infow("Successfully processed message ", "taskId", msg.TaskId, "data", msg.Data, "brokerKind", b.BrokerKind)
	}

	if err := b.imp.impAckMsg(messageWrapper); err != nil {
		err2 := core.NewFunboostRunError(fmt.Sprintf("Failed to ack message (TaskId: %s): %v, Data: %+v",
			msg.TaskId, err, msg.Data), 0, err, b.Logger)
		err2.Log()
	}

}
