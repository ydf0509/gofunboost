package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gofunboost/core"
	"go.uber.org/zap"
)

// KafkaBroker 实现Kafka消息队列
type KafkaBroker struct {
	*BaseBroker
	producer sarama.SyncProducer
	consumer sarama.ConsumerGroup
}

func (b *KafkaBroker) createKafkaProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{b.BrokerConfig.BrokerUrl}, config)
	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Could not create Kafka producer: %v", err), 0, err, b.Logger)
		err2.Log()
		return nil, err2
	}
	b.Sugar.Info("Connected to Kafka producer")
	return producer, nil
}

func (b *KafkaBroker) createKafkaConsumer() (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// 从BrokerTransportOptions中获取消费者组ID，如果未配置则使用QueueName
	groupID := "gofunboostDefaultGroupID"
	if val, ok := b.BrokerConfig.BrokerTransportOptions["group_id"].(string); ok && val != "" {
		groupID = val
	}

	consumer, err := sarama.NewConsumerGroup([]string{b.BrokerConfig.BrokerUrl}, groupID, config)
	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Could not create Kafka consumer: %v", err), 0, err, b.Logger)
		err2.Log()
		return nil, err2
	}
	b.Sugar.Info("Connected to Kafka consumer group")
	return consumer, nil
}

func (b *KafkaBroker) newBrokerCustomInit() {
	b.Sugar.Infof("newKafkaBrokerCustomInit %v", b)
	var err error
	b.producer, err = b.createKafkaProducer()
	if err != nil {
		panic(err)
	}

	b.consumer, err = b.createKafkaConsumer()
	if err != nil {
		panic(err)
	}
}

func (b *KafkaBroker) impConsumeUsingOneConn() error {
	b.Logger.Info("Kafka connection established", zap.String("addr", b.BrokerConfig.BrokerUrl))

	ctx := context.Background()
	topics := []string{b.QueueName}

	for {
		err := b.consumer.Consume(ctx, topics, &kafkaConsumerHandler{
			broker: b,
		})
		if err != nil {
			err2 := core.NewBrokerNetworkError(fmt.Sprintf("Error from consumer: %v", err), 0, err, b.Logger)
			err2.Log()
			// return err2
		}
	}
}

func (b *KafkaBroker) impSendMsg(msg string) error {
	message := &sarama.ProducerMessage{
		Topic:     b.QueueName,
		Value:     sarama.StringEncoder(msg),
		Timestamp: time.Now(),
	}

	_, _, err := b.producer.SendMessage(message)
	if err != nil {
		err2 := core.NewBrokerNetworkError(fmt.Sprintf("Failed to publish message to Kafka: %v", err), 0, err, b.Logger)
		err2.Log()
		return err2
	}

	return nil
}

func (b *KafkaBroker) impAckMsg(msgWrapper *core.MessageWrapper) error {
	if session, ok := msgWrapper.ContextExtra["session"].(sarama.ConsumerGroupSession); ok {
		if message, ok := msgWrapper.ContextExtra["message"].(*sarama.ConsumerMessage); ok {
			session.MarkMessage(message, "")
			return nil
		}
	}
	return fmt.Errorf("failed to get Kafka session or message from context")
}

// kafkaConsumerHandler 实现sarama.ConsumerGroupHandler接口
type kafkaConsumerHandler struct {
	broker *KafkaBroker
}

func (h *kafkaConsumerHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *kafkaConsumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *kafkaConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// 反序列化消息
		msg := h.broker.imp.Json2Message(string(message.Value))
		if msg == nil {
			h.broker.Sugar.Error("Failed to deserialize message")
			continue
		}

		// 使用协程池处理消息
		h.broker.Pool.Submit(func() {
			h.broker.run(&core.MessageWrapper{
				Msg: msg,
				ContextExtra: map[string]interface{}{
					"session": session,
					"message": message,
				},
			})
		})
	}
	return nil
}
