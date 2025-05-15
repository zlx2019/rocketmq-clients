from rocketmq.client import Producer, Message

###
### https://github.com/apache/rocketmq-client-python/tree/master

topic = 'example-python-topic'
group = 'example-python-group'
name_srv = '118.145.206.25:9876'

# create message
def create_message(n):
    msg = Message(topic)
    # msg.set_keys('XXX')
    # msg.set_tags('XXX')
    # msg.set_property('property', 'test')
    msg.set_body(f"message body {n}")
    return msg

# send sync message
def send_message_sync(count):
    producer = Producer(group)
    producer.set_name_server_address(name_srv)
    producer.start()
    for n in range(count):
        msg = create_message(n)
        ret = producer.send_sync(msg)
        print ('send message status: ' + str(ret.status) + ' msgId: ' + ret.msg_id)
    print ('send sync message done')
    producer.shutdown()

if __name__ == '__main__':
    send_message_sync(10)