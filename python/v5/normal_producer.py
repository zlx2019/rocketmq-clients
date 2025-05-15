### RocketMq - V5(Grpc)
### Simple Producer
###
### pip install rocketmq-python-client
### https://github.com/apache/rocketmq-clients/tree/master/python

from rocketmq import ClientConfiguration, Credentials, Message, Producer

endpoint = '118.145.206.25:8081'
topic = 'example-python-topic'


if __name__ == '__main__':
    # auth
    credentials = Credentials()
    config = ClientConfiguration(endpoint, credentials, request_timeout=5)
    # create producer instance
    producer = Producer(config, (topic,))
    try:
        # conn broker
        producer.startup()
        try:
            msg = Message()
            # 设置主题
            msg.topic = topic
            msg.body = "Hello rocketmq!".encode("utf-8")
            # msg.tag = ""
            # msg.keys = ""
            # msg.add_property("", "")
            resp = producer.send(msg)
            print(f"{producer.__str__()} send messages success: {resp}")
            # 关闭生产者
            producer.shutdown()
        except Exception as e:
            print(f"normal producer example raise exception: {e}")
            producer.shutdown()
    except Exception as e:
        print(f"{producer.__str__()} startup raise exception: {e}")
        producer.shutdown()

