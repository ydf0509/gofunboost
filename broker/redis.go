package broker

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
)

// RedisBroker 实现基于Redis的消息队列
type RedisBroker struct {
	client *redis.Client
	ctx    context.Context
}

// NewRedisBroker 创建一个新的Redis消息队列实例
func NewRedisBroker(config Config) (*RedisBroker, error) {
	if config.Address == "" {
		return nil, errors.New("Redis地址不能为空")
	}

	client := redis.NewClient(&redis.Options{
		Addr:     config.Address,
		Password: config.Password,
		DB:       config.DB,
	})

	ctx := context.Background()
	// 测试连接
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}

	return &RedisBroker{
		client: client,
		ctx:    ctx,
	}, nil
}

// Push 将任务推送到Redis队列
func (r *RedisBroker) Push(queueName string, data []byte) error {
	return r.client.LPush(r.ctx, queueName, data).Err()
}

// Pop 从Redis队列中获取任务
func (r *RedisBroker) Pop(queueName string) ([]byte, error) {
	result, err := r.client.RPop(r.ctx, queueName).Bytes()
	if err == redis.Nil {
		return nil, nil // 队列为空
	}
	return result, err
}

// Consume 消费Redis队列中的任务
func (r *RedisBroker) Consume(queueName string, handler func([]byte) error) error {
	for {
		// 使用BRPOP阻塞等待任务
		result, err := r.client.BRPop(r.ctx, 0, queueName).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			return err
		}

		// result[0]是队列名，result[1]是值
		if len(result) < 2 {
			continue
		}

		err = handler([]byte(result[1]))
		if err != nil {
			// 处理失败，可以考虑重新入队或记录错误
			return err
		}
	}
}

// Close 关闭Redis连接
func (r *RedisBroker) Close() error {
	return r.client.Close()
}