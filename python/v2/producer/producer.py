from rocketmq.client import Producer, Message

########################################################################
### RocketMQ Python V2 生产者
### https://github.com/apache/rocketmq-client-python/tree/master
########################################################################

topic = 'example_python_topic'
group = 'default_producer_group'
name_srv = '118.145.206.25:9876'

def create_message(n):
    """
    创建消息
    :param n:
    :return:
    """
    msg = Message(topic)
    # msg.set_keys('XXX')
    # msg.set_tags('XXX')
    # msg.set_property('property', 'test')
    msg.set_body(f"message body {n}")
    return msg

def send_message_sync(count):
    """
    发送同步消息
    :param count:
    :return:
    """
    # 创建生产者
    producer = Producer(group)
    producer.set_name_server_address(name_srv)
    producer.start()
    for n in range(count):
        msg = create_message(n)
        ret = producer.send_sync(msg)
        print ('send message status: ' + str(ret.status) + ' msgId: ' + ret.msg_id)
    print ('send sync message done')
    # 关闭生产者
    producer.shutdown()

def send_orderly_message(count):
    """
    发送顺序消息，消费者也会按照顺序消费
    :param count:
    :return:
    """
    # 创建生产者，顺序发送
    producer = Producer(group, orderly=True, timeout=2000)
    producer.set_name_server_address(name_srv)
    producer.start()
    for n in range(count):
        msg = create_message(n)
        ret = producer.send_orderly_with_sharding_key(msg, f"message Key {n}")
        print('send orderly message status: ' + str(ret.status) + ' msgId: ' + ret.msg_id)
    print ('send orderly message done')
    producer.shutdown()



def send_tx_message():
    """
    发送事务消息
    :return:
    """
    pass

if __name__ == '__main__':
    send_message_sync(10)
    # send_orderly_message(10)