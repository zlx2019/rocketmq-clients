/* CSimpleProducer.c -- RocketMQ producer for C.
 *	- SyncMessage
 *	-
 *
 */

#include "CCommon.h"
#include "CMessage.h"
#include "CProducer.h"
#include "CSendResult.h"
#include "common.h"

#include <stdio.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <memory.h>
#include <unistd.h>
#
#endif

// MQ Topic
const char *NAME_SRV = "118.145.206.25:9876";
const char *TOPIC = "example_cpp_topic";
const char *GROUP = "default_producer_group";

void SendMessages(CProducer *producer, int count);

// RocketMQ Producer by C语言版本
//
int main(int argc, char *argv[]) {
	// 创建生产者
	CProducer* producer = CreateProducer(GROUP);
	SetProducerNameServerAddress(producer, NAME_SRV);

	// 启动生产者
	StartProducer(producer);
	printf("Producer startup! \n");

	// 发送消息
	SendMessages(producer, 10);

	// 关闭并释放生产者
	ShutdownProducer(producer);
	DestroyProducer(producer);
	printf("Producer stopped! \n");
}

/**
 * 发送消息
 * @param producer 生产者
 */
void SendMessages(CProducer *producer, int count) {
	// 创建消息
	char payload[1024];
	CMessage *message = CreateMessage(TOPIC);
	// 设置消息属性
	// SetMessageTags(message, "tags");
	// SetMessageKeys(message, "keys");
	// SetMessageProperty(message, "key", "value");

	// 发送同步消息
	CSendResult sendResult;
	for (int i = 0; i < count; i++) {
		memset(payload, 0, sizeof(payload));
		snprintf(payload, sizeof(payload), "This is the %d message", i);
		SetMessageBody(message, payload);
		const int status = SendMessageSync(producer, message, &sendResult);
		if (status == OK) {
			printf("send message[%d] result status:%d, msgId:%s\n", i, (int) sendResult.sendStatus, sendResult.msgId);
		} else {
			printf("send message[%d] failed !\n", i);
		}
		// 睡眠1s
		t_sleep(1000);
	}
	// 释放消息
	DestroyMessage(message);
}