### RocketMq - V5(Grpc)
### Simple Consumer
###
### pip install rocketmq-python-client
### https://github.com/apache/rocketmq-clients/tree/master/python

from rocketmq import ClientConfiguration, Credentials, SimpleConsumer

endpoints = '118.145.206.25:8081'
topic = 'example-python-topic'
group = 'example-python-group'


if __name__ == '__main__':
    credentials = Credentials()
    # if auth enable
    # credentials = Credentials("ak", "sk")
    config = ClientConfiguration(endpoints, credentials)
    simple_consumer = SimpleConsumer(config, group)
    try:
        simple_consumer.startup()
        try:
            simple_consumer.subscribe(topic)
            # use tag filter
            # simple_consumer.subscribe(topic, FilterExpression("tag"))
            while True:
                try:
                    # max message num for each long polling and message invisible duration after it is received
                    messages = simple_consumer.receive(32, 15)
                    if messages is not None:
                        print(f"{simple_consumer.__str__()} receive {len(messages)} messages.")
                        for msg in messages:
                            print(f"{simple_consumer.__str__()} ack message:[{msg.message_id}].")
                            print(f"consumer message: {msg.body}")
                            simple_consumer.ack(msg)
                except Exception as e:
                    print(f"receive or ack message raise exception: {e}")
        except Exception as e:
            print(f"{simple_consumer.__str__()} subscribe topic:{topic} raise exception: {e}")
            simple_consumer.shutdown()
    except Exception as e:
        print(f"{simple_consumer.__str__()} startup raise exception: {e}")
        simple_consumer.shutdown()