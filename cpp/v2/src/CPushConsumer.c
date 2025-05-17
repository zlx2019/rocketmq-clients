/* CPushConsumer.c.c -- RocketMQ producer for C.
 *	- PushConsumer
 *
 */

#include <signal.h>
#include <stdbool.h>
#include <unistd.h>
#include <stdlib.h>
#include "common.h"
#include "CPushConsumer.h"
#include "CMessageExt.h"

const char *NAME_SRV = "118.145.206.25:9876";
const char *TOPIC = "example_cpp_topic";
const char *GROUP = "example_cpp_topic_consumer_group";

volatile bool running = true;
CPushConsumer* consumer = NULL;

/// 信号接收处理
void signalHandler(int sig) {
	printf("\nReceive signal %d, shutdown... \n", sig);
	running = false;
}

/// 绑定需要捕获的信号
void bindSignalHandlers() {
	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = signalHandler;

	sigaction(SIGINT, &sa, NULL); // Ctrl+C 信号
	sigaction(SIGTERM, &sa, NULL); // kill信号
	sigaction(SIGHUP, &sa, NULL); // 终端关闭信号
}

/// 资源释放
void cleanup() {
	if (consumer != NULL) {
		ShutdownPushConsumer(consumer);
		DestroyPushConsumer(consumer);
		consumer = NULL;
		printf("Consumer cleanup and shutdown. \n");
	}
}

/// 消息处理函数
int handlerMessage(struct CPushConsumer* consumer, CMessageExt* ext) {
	const char* msgId = GetMessageId(ext);
	const char* body = GetMessageBody(ext);
	printf("consumer msgId: %s, content: %s \n", msgId, body);
	return E_CONSUME_SUCCESS;
}

/// RocketMQ 消费者
int main(int argc, char *argv[]) {
	// 注册信号处理 & 退出钩子函数
	bindSignalHandlers();
	atexit(cleanup);
	// 创建消费者
	consumer = CreatePushConsumer(GROUP);
	if (consumer == NULL) {
		printf("Failed to create a consumer \n");
		return EXIT_FAILURE;
	}
	SetPushConsumerNameServerAddress(consumer, NAME_SRV);
	// 订阅主题
	if (Subscribe(consumer, TOPIC, "*") != 0) {
		printf("Failed to subscribe topic! \n");
		return EXIT_FAILURE;
	}
	// 注册消息处理函数
	if (RegisterMessageCallback(consumer, handlerMessage) != 0) {
		printf("Failed to register message callback! \n");
		return EXIT_FAILURE;
	}
	if (StartPushConsumer(consumer) != 0) {
		printf("Failed to start push consumer! \n");
		return EXIT_FAILURE;
	}
	printf("Consumer Startup! \n");
	while (running) {
		t_sleep(1000);
	}
	cleanup();
	return 0;
}