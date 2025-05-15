use rocketmq_rust::rocketmq;


/// simple producer
///

pub const MESSAGE_COUNT: usize = 1;
pub const TOPIC: &str = "example-rust-topic";
pub const PRODUCER_GROUP: &str = "default_producer_group";
pub const NAME_SERVER_ADDR: &str = "118.145.206.25:9876";
// pub const TAG: &str = "TagA";

#[rocketmq::main]
async fn main() {
    println!("Hello, world!");
}