use std::process::exit;
use rocketmq::conf::{ClientOption, ProducerOption};
use rocketmq::model::message::MessageBuilder;
use rocketmq::Producer;

/// RocketMq Rust v5
/// Simple Producer

pub const TOPIC: &str = "example-golang-topic";
pub const ENDPOINT: &str = "118.145.206.25:8081";

#[tokio::main]
async fn main() {
    // producer option
    let mut prod_opt = ProducerOption::default();
    prod_opt.set_topics(vec![TOPIC]);

    // client option
    let mut cli_opt = ClientOption::default();
    cli_opt.set_access_url(ENDPOINT);

    // create producer
    let mut producer = Producer::new(prod_opt, cli_opt).unwrap();
    producer.start().await.unwrap();

    let msg = MessageBuilder::builder()
        .set_topic(TOPIC)
        // .set_message_group(PRODUCER_GROUP)
        // .set_tag()
        // .set_keys()
        // .set_properties()
        .set_body("Hello Rust!".as_bytes().to_vec())
        .build().unwrap();
    // send message
    let send_result = producer.send(msg).await.unwrap();
    println!("message {} send success", send_result.message_id());

    // shutdown producer
    producer.shutdown().await.unwrap();
}
