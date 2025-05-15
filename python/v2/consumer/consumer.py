# -*- coding: utf-8 -*-
#
########################################################################
### RocketMQ Python V2 消费者
### https://github.com/apache/rocketmq-client-python/tree/master
########################################################################
import signal
import time

from rocketmq.client import PushConsumer, ConsumeStatus
from rocketmq.ffi import MessageModel

topic = 'example_python_topic'
group = 'example_python_consumer_group'
name_srv = '118.145.206.25:9876'

def consumer_listener():
    """
    消息监听器
    :return:
    """
    # 创建消费者
    consumer = PushConsumer(group, message_model=MessageModel.CLUSTERING)
    consumer.set_name_server_address(name_srv)
    # 订阅主题
    consumer.subscribe(topic, message_handle, '*')
    try:
        # 启动消费者
        consumer.start()
        # 优雅停机
        stop = False
        def quit_signal_handler(sig, frame):
            nonlocal stop
            print(f"Received {sig} signal done.")
            stop = True
        # 注册信号处理器
        signal.signal(signal.SIGINT, quit_signal_handler)
        signal.signal(signal.SIGTERM, quit_signal_handler)
        # 等待结束信号
        while not stop:
            time.sleep(1)

    except Exception as e:
        print(f"consumer error: {e}")
    finally:
        # 关闭消费者
        consumer.shutdown()


def message_handle(message):
    """
    消息处理函数
    :param message:
    :return:
    """
    # print(message.id, message.body, message.get_property('property'))
    body = message.body.decode('utf-8')
    print(f"consumer message: {body}")
    return ConsumeStatus.CONSUME_SUCCESS


if __name__ == '__main__':
    consumer_listener()