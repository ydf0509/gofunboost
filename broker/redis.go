package broker

import (
	"context"
	// "encoding/json"
	// "reflect"

	// "errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/ydf0509/gofunboost/core"
	"go.uber.org/zap"
	// "golang.org/x/time/rate"
)

// RedisBroker 实现Redis消息队列
type RedisBroker struct {
	*BaseBroker
	RedisConn *redis.Client
}

func (b *RedisBroker) createRedisClient() (*redis.Client, error) {
	ctx := context.Background()
	// 创建带连接池配置的 Redis 客户端
	rdb := redis.NewClient(&redis.Options{
		Addr:         b.BrokerConfig.BrokerUrl,
		DB:           b.BrokerConfig.BrokerTransportOptions["DB"].(int),
		PoolSize:     100, // 最大连接数
		MinIdleConns: 3,   // 最小闲置连接数
	})

	// 测试连接
	_, err := rdb.Ping(ctx).Result()
	if err != nil {

		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Could not connect to Redis: %v", err), 0, err, b.Logger)
		err2.Log()
		return rdb, err
	} else {
		b.Sugar.Info("Connected to Redis")
	}
	return rdb, nil
}

func (b *RedisBroker) newBrokerCustomInit() {
	b.Sugar.Infof("newReidsBrokerCustomInit %v", b)
	var err error
	b.RedisConn, err = b.createRedisClient()
	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Could not connect to Redis: %v", err), 0, err, b.Logger)
		err2.Log()
		panic(err2)
	}
}

// ConsumeUsingOneConn 使用一个连接消费消息
func (b *RedisBroker) impConsumeUsingOneConn() error {

	b.Logger.Info("Redis connection established", zap.String("addr", b.RedisConn.Options().Addr))

	ctx := context.Background()
	// 持续消费消息
	for {

		// 从队列中获取消息
		result, err := b.RedisConn.BLPop(ctx, time.Second*60, b.QueueName).Result()
		if err != nil {
			if err == redis.Nil {
				// 队列为空，等待一段时间后重试
				time.Sleep(100 * time.Millisecond)
				continue
			}
			err2 := core.NewBrokerNetworkError(fmt.Sprintf("Failed to get message from Redis: %v", err), 0, err, b.Logger)
			err2.Log()
			return err2
		}

		// BLPOP返回一个包含key和value的数组，value在索引1
		msgStr := result[1]

		// 反序列化消息
		msg := b.imp.Json2Message(msgStr)
		// 获取消费函数的参数类型

		// 使用协程池处理消息
		b.Pool.Submit(func() { b.execute(&core.MessageWrapper{Msg: msg}) })
	}
}

func (b *RedisBroker) Clear() error {
	ctx := context.Background()
	// 删除整个队列
	_, err := b.RedisConn.Del(ctx, b.QueueName).Result()
	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Failed to clear Redis queue: %v", err), 0, err, b.Logger)
		err2.Log()
		return err2
	}
	b.Sugar.Warnf("Successfully cleared Redis queue: %s", b.QueueName)
	return nil
}

func (b *RedisBroker) impSendMsg(msg string) error {
	// 使用RPush将消息推送到队列
	ctx := context.Background()
	_, err := b.RedisConn.RPush(ctx, b.QueueName, msg).Result()
	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Failed to push message to Redis: %v", err), 0, err, b.Logger)
		err2.Log()
		return err2
	}

	return nil
}

func (b *RedisBroker) impAckMsg(msg *core.MessageWrapper) error {
	b.Sugar.Info("redis not support ack")
	return nil //redis not support ack
}
