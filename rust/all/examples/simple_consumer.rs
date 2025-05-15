use anyhow::Context;
use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use rocketmq_client_rust::consumer::message_selector::MessageSelector;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use tracing::info;

///
/// RocketMQ Rust 消费者（V2）
///

const TOPIC: &'static str = "example-rust-topic";
const CONSUMER_GROUP: &'static str = "example-rust-consumer-group";
const NAME_SRV_ADDR: &'static str = "118.145.206.25:9876";
const TAGS: &'static str = "*";


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // rocketmq_common::log::init_logger();
    // 创建消费者
    let builder = DefaultMQPushConsumer::builder();
    let mut consumer = builder
        .name_server_addr(NAME_SRV_ADDR) // NameServer
        .consumer_group(CONSUMER_GROUP) // 消费组
        .message_model(MessageModel::Clustering) // 消息消费模式
        .build();
    // 订阅主题
    consumer.subscribe(TOPIC, TAGS).with_context(|| format!("failed to subscribe to {}", TOPIC))?;

    // 使用SQl92
    // consumer.subscribe_with_selector(TOPIC, Some(MessageSelector::by_sql("age is null")))?;

    // 注册消息处理器（并发消费）
    consumer.register_message_listener_concurrently(MessageListener);
    // 启动消费者
    consumer.start().await.with_context(|| format!("failed to start consumer for {}", TOPIC))?;
    info!("Consumer listen success");
    // wait
    let _ = tokio::signal::ctrl_c().await;
    consumer.shutdown().await;
    Ok(())
}

/// 实现消息监听器
struct MessageListener;
impl MessageListenerConcurrently for MessageListener {
    fn consume_message(&self, messages: &[&MessageExt], _: &ConsumeConcurrentlyContext) -> rocketmq_client_rust::Result<ConsumeConcurrentlyStatus> {
        for message in messages {
            // info!("Consumer message: {:?}", message);
            // let s = message.msg_id;
            let body = match message.get_body() {
                Some(bytes) => {
                    // 将 bytes 转换为字符串 utf8
                    match std::str::from_utf8(bytes) {
                        Ok(msg) => String::from(msg),
                        Err(_) => {
                            // 非utf8格式
                            // 用lossy转换，会将无效UTF-8序列替换为�符号
                            String::from_utf8_lossy(bytes).into_owned()
                        },
                    }
                },
                None => {
                    // 消息体为空
                    String::new()
                }
            };
            println!("Consumer message: {}", body);
        }
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}