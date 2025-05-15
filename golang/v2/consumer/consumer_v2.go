package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"log"
	"os"
	"os/signal"
	"syscall"
)

/// RocketMQ Golang Remoting Client
/// `go get github.com/apache/rocketmq-client-go/v2`
/// consumer example: https://github.com/apache/rocketmq-client-go/tree/master/examples/consumer

const (
	nameServer         = "118.145.206.25:9876"
	ReCaptchaTaskTopic = "example-golang-topic" // 主题名
	ReCaptchaTaskGroup = "example-golang-group" // 消费组
)

func main() {
	signals := make(chan os.Signal)
	// 创建消费者, 可选 Pull | Push 两种模式
	reConsumer, err := rocketmq.NewPushConsumer(
		consumer.WithNameServer([]string{nameServer}),
		consumer.WithGroupName(ReCaptchaTaskGroup),
		consumer.WithConsumeGoroutineNums(16),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithAutoCommit(true),
	)
	if err != nil {
		log.Fatalf("consumer connect : %v", err)
	}
	// subscribe topics
	if err = reConsumer.Subscribe(ReCaptchaTaskTopic, consumer.MessageSelector{}, messageHandler); err != nil {
		log.Fatalf("subscribe topic: %v", err.Error())
	}
	// start consumer
	if err = reConsumer.Start(); err != nil {
		log.Fatalf("startup consumer: %v", err.Error())
	}
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-signals
	if err = reConsumer.Shutdown(); err != nil {
		log.Fatalf("consumer shutdown: %v", err.Error())
	}
	log.Println("consumer stopped...")
}

// 消息处理函数
func messageHandler(ctx context.Context, messages ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	for _, msg := range messages {
		select {
		case <-ctx.Done():
			// 上下文中断
			log.Println("consumer interrupted...")
			return consumer.ConsumeRetryLater, ctx.Err()
		default:
			fmt.Printf("consume message: %s\n", string(msg.Body))
		}
	}
	return consumer.ConsumeSuccess, nil
}
