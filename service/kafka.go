package service

import (
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type Kafka struct {
	KafkaBrokers    []string
	KafkaDataTopic  string
	KafkaEventTopic string
	DataPubClient   PubClient
	EventPubClient  PubClient
}

func (k *Kafka) Init() error {
	k.DataPubClient = NewPubClient(kafka.WriterConfig{
		Brokers:  k.KafkaBrokers,
		Topic:    k.KafkaDataTopic,
		Balancer: &kafka.Hash{},
	})

	k.EventPubClient = NewPubClient(kafka.WriterConfig{
		Brokers:  k.KafkaBrokers,
		Topic:    k.KafkaEventTopic,
		Balancer: &kafka.Hash{},
	})

	return nil
}

func (k *Kafka) SendDataMessage(key string, data []byte) error {
	err := k.DataPubClient.Send(context.Background(), kafka.Message{
		Key:   []byte(key),
		Value: data,
	})
	if err != nil {
		logrus.Error(err.Error())
		return err
	}

	return nil
}

func (k *Kafka) SendEventMessage(key string, data []byte) error {
	err := k.EventPubClient.Send(context.Background(), kafka.Message{
		Key:   []byte(key),
		Value: data,
	})
	if err != nil {
		logrus.Error(err.Error())
		return err
	}

	return nil
}

func (k *Kafka) Close() error {
	err := k.EventPubClient.Close()
	if err != nil {
		logrus.Errorf("close event pubClient err:%s", err.Error())
		return err
	}
	err = k.DataPubClient.Close()
	if err != nil {
		logrus.Errorf("close data pubClient err:%s", err.Error())
		return err
	}

	return nil
}

type PubClient interface {
	Send(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type pubClient struct {
	writer *kafka.Writer
}

func NewPubClient(config kafka.WriterConfig) PubClient {
	writer := kafka.NewWriter(config)
	return &pubClient{writer: writer}
}

func (c *pubClient) Send(ctx context.Context, msgs ...kafka.Message) error {
	return c.writer.WriteMessages(ctx, msgs...)
}

func (c *pubClient) Close() error {
	return c.writer.Close()
}
