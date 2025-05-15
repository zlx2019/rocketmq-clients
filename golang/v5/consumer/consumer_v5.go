package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

/// RocketMQ Golang Grpc client consumer
/// `go get github.com/apache/rocketmq-clients/golang/v5`

const (
	Topic              = "example-golang-topic" // 主题名
	TopicConsumerGroup = "example-golang-group" // 消费组
	Endpoint           = "118.145.206.25:8081"  // Broker 代理地址
)

const (
	// maximum waiting time for receive func
	AwaitDuration           = time.Second * 5
	PullMaxMessageNum int32 = 50 // 单次拉取的最大消息数
	// invisibleDuration should > 20s
	InvisibleDuration = time.Second * 20
)

func main() {
	os.Setenv("mq.consoleAppender.enabled", "true")
	// MQ Config
	cf := &golang.Config{
		Endpoint:      Endpoint,
		ConsumerGroup: TopicConsumerGroup,
		Credentials: &credentials.SessionCredentials{
			AccessKey:     "",
			AccessSecret:  "",
			SecurityToken: "",
		},
	}
	// 创建消费者
	consumer, err := golang.NewSimpleConsumer(cf,
		golang.WithAwaitDuration(AwaitDuration),
		golang.WithSubscriptionExpressions(map[string]*golang.FilterExpression{
			Topic: golang.SUB_ALL,
		}),
	)
	if err != nil {
		log.Fatalf("create consumer error: %v", err)
	}
	// 启动消费者
	if err = consumer.Start(); err != nil {
		log.Fatalf("consumer start error: %v", err)
	}
	signals := make(chan os.Signal)
	defer consumer.GracefulStop()
	go func() {
		for {
			// 消费消息
			messages, err := consumer.Receive(context.Background(), PullMaxMessageNum, InvisibleDuration)
			if e, ok := golang.AsErrRpcStatus(err); ok {
				if e.GetCode() == int32(v2.Code_MESSAGE_NOT_FOUND) {
					fmt.Println("暂时没有消息...")
					continue
				}
			}
			if err != nil {
				log.Fatalf("pull message error: %v", err)
			}
			// TODO handler message
			for _, msg := range messages {
				fmt.Printf("consumer message: %v \n", string(msg.GetBody()))
				// 消费成功后, ACK
				consumer.Ack(context.Background(), msg)
			}
		}
	}()
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-signals
}
