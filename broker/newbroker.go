package broker

import (
	"fmt"
	"reflect"
	"time"

	gopool "github.com/ydf0509/gofunboost/concurrentpool/simplepool"
	"github.com/ydf0509/gofunboost/core"
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
		Pool:         gopool.NewGoPool(boostoptions.ConcurrentNum),
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
	// 检查结构体字段是否都是可导出的（大写开头）
	if base.ParamType.Kind() == reflect.Struct {
		for i := 0; i < base.ParamType.NumField(); i++ {
			field := base.ParamType.Field(i)
			if field.PkgPath != "" {
				panic(core.NewFunboostRunError(fmt.Sprintf("struct field '%s' must be exported (start with uppercase) for JSON serialization", field.Name), 0, nil, nil))
			}
		}
	}
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
	case core.SQLITE:
		sqliteBroker := &SqliteBroker{BaseBroker: base}
		sqliteBroker.imp = sqliteBroker
		broker = sqliteBroker
	default:
		base.imp = base
		broker = base
	}

	broker.newBrokerCustomInit()
	return broker
}
