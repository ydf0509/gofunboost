package broker

import (
	"context"
	"encoding/json"

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

	// 持续消费消息
	for {
		// 使用blpop 获取消息，添加到协程池
		ctx := context.Background()

		// 如果设置了QPS限制，则等待令牌
		if b.limiter != nil {
			b.limiter.Wait(ctx)
		}

		// 从队列中获取消息
		result, err := client.BLPop(ctx, 60, b.QueueName).Result()
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

		// 使用协程池处理消息
		b.Pool.Submit(func() {
			if err := b.ConsumeFunc(&msg); err != nil {
				b.Logger.Errorf("Failed to process message: %v", err)
			}
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
