package broker

import (
	"reflect"
	"time"
	"github.com/gofunboost/concurrentpool"
	"github.com/gofunboost/core"
	"golang.org/x/time/rate"
)

// NewBroker 创建一个新的Broker
func NewBroker(boostoptions core.BoostOptions) Broker {
	if boostoptions.QueueName == "" {
		panic("queue name is required")
	}

	if boostoptions.ConsumeFunc == nil {
		panic(core.NewFunboostRunError("consume func is required", 0, nil, nil))
	}

	base := &BaseBroker{
		BoostOptions: boostoptions,
		Pool:         concurrentpool.NewGoEasyPool(boostoptions.ConcurrentNum),
		StartTimeTs:  time.Now().Unix(), 
	}

	if base.QPSLimit > 0 {
		base.limiter = rate.NewLimiter(rate.Limit(base.QPSLimit), 1)
	}

	base.Sugar = boostoptions.Logger.Sugar()

	// 使用反射调用消费函数
	base.FuncValue = reflect.ValueOf(boostoptions.ConsumeFunc)
	base.FuncType = reflect.TypeOf(boostoptions.ConsumeFunc)

	// 创建一个新的目标类型实例
	base.ParamType = base.FuncType.In(0)
	// base.ParamValue = reflect.New(paramType).Interface()

	var broker Broker
	switch boostoptions.BrokerKind {
	case core.REDIS:
		redisBroker := &RedisBroker{BaseBroker: base}
		redisBroker.imp = redisBroker
		broker = redisBroker
	case core.KAFKA:
		kafkaBroker := &KafkaBroker{BaseBroker: base}
		kafkaBroker.imp = kafkaBroker
		broker = kafkaBroker
	case core.RABBITMQ:
		rabbitBroker := &RabbitMQBroker{BaseBroker: base}
		rabbitBroker.imp = rabbitBroker
		broker = rabbitBroker
	case core.MEMORY:
		memoryBroker := &MemoryBroker{BaseBroker: base}
		memoryBroker.imp = memoryBroker
		broker = memoryBroker
	default:
		base.imp = base
		broker = base
	}

	broker.newBrokerCustomInit()
	return broker
}