package main

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/google/uuid"
	"log"
	"strconv"
)

const (
	NameServer    = "118.145.206.25:9876"    // NameServer
	Topic         = "example-client-golang"  // 主题
	ProducerGroup = "default_producer_group" // 生产组
)

func main() {
	sender, err := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{NameServer})),
		producer.WithRetry(3),
		producer.WithGroupName(ProducerGroup),
	)
	if err != nil {
		log.Fatalf("create producer: %v", err.Error())
	}
	if err = sender.Start(); err != nil {
		log.Fatalf("startup producer: %v", err.Error())
	}
	defer sender.Shutdown()
	for i := 1; i <= 10; i++ {
		msg := &primitive.Message{
			Topic: Topic,
			Body:  []byte("Hello RocketMQ " + strconv.Itoa(i)),
		}
		// with key
		msg.WithKeys([]string{uuid.New().String()})
		sendResult, err := sender.SendSync(context.Background(), msg)
		if err != nil {
			log.Fatalf("send message: %v", err)
		}
		log.Println(sendResult.Status)
	}
}
