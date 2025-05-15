use anyhow::Context;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_single::Message;

///
/// RocketMQ Rust 生产者（V2）
///

const TOPIC: &'static str = "example-rust-topic";
const PRODUCER_GROUP: &'static str = "default_producer_group";
const NAME_SRV_ADDR: &'static str = "118.145.206.25:9876";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // init log
    rocketmq_common::log::init_logger();

    // create a producer
    let mut producer = DefaultMQProducer::builder()
        .name_server_addr(NAME_SRV_ADDR)
        .producer_group(PRODUCER_GROUP)
        .max_message_size(4096)
        .auto_batch(true)
        .build();

    // connect
    producer.start().await.with_context(|| "producer startup fail")?;
    // send messages
    for i in 0..10 {
        // create message
        let mut message = Message::new(TOPIC, format!("this is the {i} message").as_bytes());
        // ext
        // message.set_keys();
        // message.set_tags();
        // message.set_properties();
        let send_result = producer.send_with_timeout(message, 2000).await.with_context(|| "message send fail");
        println!("send {i} message: {:?}", send_result);
    }
    // shutdown
    producer.shutdown().await;
    Ok(())
}