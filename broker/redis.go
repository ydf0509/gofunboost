package broker

import (
	"context"
	// "encoding/json"
	// "reflect"

	// "errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gofunboost/core"
	"go.uber.org/zap"
	// "golang.org/x/time/rate"
)

// RedisBroker 实现Redis消息队列
type RedisBroker struct {
	*BaseBroker
	RedisConn *redis.Client
}

func NewRedisBroker(boostoptions core.BoostOptions) *RedisBroker {
	return NewBroker(boostoptions).(*RedisBroker)
}

// createRedisClient 创建Redis客户端
const (
	errCreateRedisClient = "failed to create Redis client: %v"
)

func (b *RedisBroker) createRedisClient() (*redis.Client, error) {
	ctx := context.Background()
	// 创建带连接池配置的 Redis 客户端
	rdb := redis.NewClient(&redis.Options{
		Addr:         b.BrokerConfig.BrokerUrl,
		PoolSize:     100, // 最大连接数
		MinIdleConns: 3,   // 最小闲置连接数
	})

	// 测试连接
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		err2:= &core.FunboostBrokerNetworkError{FunboostError: core.FunboostError{
			Message: fmt.Sprintf("Could not connect to Redis: %v", err),
			Logger: b.Logger,
		}}
		err2.Log()
		return rdb, err
	} else {
		b.Sugar.Info("Connected to Redis")
	}
	return rdb, nil
}

func (b *RedisBroker) newBrokerCustomInit() {
	b.Sugar.Infof("newBrokerCustomInit %v", b)
}

// ConsumeUsingOneConn 使用一个连接消费消息
func (b *RedisBroker) impConsumeUsingOneConn() error {
	// 创建Redis客户端
	client, err := b.createRedisClient()
	if err != nil {
		err2 := &core.FunboostBrokerNetworkError{FunboostError: core.FunboostError{
			Message: fmt.Sprintf("Could not connect to Redis: %v", err),
			Logger: b.Logger,
		}}
		err2.Log()
		return err2
	}
	defer client.Close()

	b.Logger.Info("Redis connection established", zap.String("addr", client.Options().Addr))

	ctx := context.Background()
	// 持续消费消息
	for {

		// 从队列中获取消息
		result, err := client.BLPop(ctx, time.Second*60, b.QueueName).Result()
		if err != nil {
			if err == redis.Nil {
				// 队列为空，等待一段时间后重试
				time.Sleep(100 * time.Millisecond)
				continue
			}
			err2 := &core.FunboostBrokerNetworkError{FunboostError: core.FunboostError{
				Message: fmt.Sprintf("Failed to get message from Redis: %v", err),
				Logger: b.Logger,
			}}
			err2.Log()
			return err2
		}

		// BLPOP返回一个包含key和value的数组，value在索引1
		msgStr := result[1]

		// 反序列化消息
		msg:=b.imp.Json2Message(msgStr)
		// 获取消费函数的参数类型

		// 使用协程池处理消息
		b.Pool.Submit(func() {b.run(&core.MessageWrapper{Msg: msg})})
	}
}

func (b *RedisBroker) impSendMsg(msg string) error {
	if b.RedisConn == nil {
		// 创建Redis客户端
		client, err := b.createRedisClient()
		if err != nil {
			err2:= &core.FunboostBrokerNetworkError{FunboostError: core.FunboostError{
				Message: fmt.Sprintf("Could not connect to Redis: %v", err),
				Logger: b.Logger,	
			}}
			err2.Log()
			return err2
		}
		b.RedisConn = client
	}

	// 使用RPush将消息推送到队列
	ctx := context.Background()
	_, err := b.RedisConn.RPush(ctx, b.QueueName, msg).Result()
	if err != nil {
		err2:= &core.FunboostBrokerNetworkError{FunboostError: core.FunboostError{
			Message: fmt.Sprintf("Failed to push message to Redis: %v", err),
			Logger: b.Logger,	
		}}
		err2.Log()
		return err2
	}

	return nil
}

func (b *RedisBroker) impAckMsg(msg *core.MessageWrapper) error {
	b.Sugar.Info("redis not support ack")
	return nil //redis not support ack
}
