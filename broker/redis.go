package broker

import (
	"context"
	"encoding/json"
	"reflect"

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
		b.Logger.Errorf("Could not connect to Redis: %v", err)
		return rdb, err
	} else {
		b.Logger.Info("Connected to Redis")
	}
	return rdb, nil
}

func (b *RedisBroker) newBrokerCustomInit() {
	b.Logger.Infof("newBrokerCustomInit %v", b)
}

// ConsumeUsingOneConn 使用一个连接消费消息
func (b *RedisBroker) impConsumeUsingOneConn() error {
	// 创建Redis客户端
	client, err := b.createRedisClient()
	if err != nil {
		b.Logger.Errorf("failed to create Redis client: %v", err)
		return fmt.Errorf("failed to create Redis client: %w", err)
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
			b.Logger.Errorf("Failed to get message from Redis: %v", err)
			return fmt.Errorf("failed to get message from Redis: %w", err)
		}

		// BLPOP返回一个包含key和value的数组，value在索引1
		msgStr := result[1]

		// 反序列化消息
		var msg core.Message
		if err := json.Unmarshal([]byte(msgStr), &msg); err != nil {
			b.Logger.Errorf("Failed to unmarshal message: %v", err)
			continue
		}

		// 如果设置了QPS限制，则等待令牌
		if b.limiter != nil {
			b.limiter.Wait(ctx)
		}

	

		// 使用协程池处理消息
		b.Pool.Submit(func() {
			// 记录消息处理开始
			// b.Logger.Infof("Processing message with TaskId: %s", msg.TaskId)

			// 使用反射调用消费函数
			funcValue := reflect.ValueOf(b.ConsumeFunc)
			// 获取函数类型
			funcType := funcValue.Type()

			// 准备函数参数
			args := make([]reflect.Value, len(msg.FucnArgs))
			for i, arg := range msg.FucnArgs {
				// 获取期望的参数类型
				expectedType := funcType.In(i)
				argValue := reflect.ValueOf(arg)

				// 如果需要类型转换
				if argValue.Type() != expectedType {
					// 处理数值类型转换
					if argValue.Kind() == reflect.Float64 && expectedType.Kind() == reflect.Int {
						args[i] = reflect.ValueOf(int(argValue.Float()))
					} else {
						// 其他类型转换场景可以在这里添加
						args[i] = argValue
					}
				} else {
					args[i] = argValue
				}
			}

			// 调用函数
			results := funcValue.Call(args)

			// 检查是否有错误返回
			if len(results) > 0 {
				lastResult := results[len(results)-1]
				if lastResult.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) && !lastResult.IsNil() {
					err := results[len(results)-1].Interface().(error)
					b.Logger.Errorf("Failed to process message (TaskId: %s): %v, Args: %+v",
						msg.TaskId, err, msg.FucnArgs)
					return
				}
			}

			b.Logger.Infof("Successfully processed message with TaskId: %s", msg.TaskId)
		})
	}
}

func (b *RedisBroker) impSendMsg(msg string) error {
	if b.RedisConn == nil {
		// 创建Redis客户端
		client, err := b.createRedisClient()
		if err != nil {
			b.Logger.Errorf("failed to create Redis client: %v", err)
			return fmt.Errorf("failed to create Redis client: %w", err)
		}
		b.RedisConn = client
	}

	// 使用RPush将消息推送到队列
	ctx := context.Background()
	_, err := b.RedisConn.RPush(ctx, b.QueueName, msg).Result()
	if err != nil {
		b.Logger.Errorf("failed to push message to Redis: %v", err)
		return fmt.Errorf("failed to push message to Redis: %w", err)
	}

	return nil
}

func (b *RedisBroker) impAckMsg(msg *core.MessageWrapper) error {
	return nil //redis not support ack
}
