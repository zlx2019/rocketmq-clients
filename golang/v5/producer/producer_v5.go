package main

import (
	"context"
	"fmt"
	mq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
	"log"
	"os"
	"strconv"
	"time"
)

//
// RocketMQ V5 Producer
//

const (
	Topic    = "example-golang-topic" // 主题名
	Endpoint = "118.145.206.25:8081"  // Broker 代理地址
)

func main() {
	_ = os.Setenv("mq.consoleAppender.enabled", "true")
	mq.ResetLogger()
	// 创建生产者
	producer, err := mq.NewProducer(
		&mq.Config{Endpoint: Endpoint, Credentials: &credentials.SessionCredentials{
			AccessKey:     "",
			AccessSecret:  "",
			SecurityToken: "",
		}},
		mq.WithTopics(Topic),
	)
	if err != nil {
		log.Fatalf("create producer: %v", err.Error())
	}
	fmt.Printf("%T \n", producer)
	// start producer
	err = producer.Start()
	if err != nil {
		log.Fatal(err)
	}
	// graceful stop producer
	defer producer.GracefulStop()
	for i := 0; i < 10; i++ {
		// new a message
		msg := &mq.Message{
			Topic: Topic,
			Body:  []byte("this is a message : " + strconv.Itoa(i)),
		}
		// set keys and tag
		//msg.SetKeys("a", "b")
		//msg.SetTag("ab")
		// send message in sync
		resp, err := producer.Send(context.TODO(), msg)
		if err != nil {
			log.Fatal(err)
		}
		for i := 0; i < len(resp); i++ {
			fmt.Printf("%#v\n", resp[i])
		}
		// wait a moment
		time.Sleep(time.Second * 1)
	}
}
