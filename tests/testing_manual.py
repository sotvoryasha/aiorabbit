import yaml
import asyncio
from rmqclient import Consumer, Config, Publisher


def callback(message):
    print(message.json())


def read_config_from_yml():
    with open('simple_topology_rmq.yml') as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)
    return config_dict


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    config = read_config_from_yml()

    conf = Config(config)
    consumer = Consumer(**conf.__dict__)
    publisher = Publisher(**conf.__dict__)
    print(config)
    loop.run_until_complete(consumer.run())
    loop.run_until_complete(publisher.run())

    loop.run_until_complete(consumer.consume_await(callback, 'test_queue2'))
    for i in range(10):
        loop.run_until_complete(publisher.send_message({1: i}, 'test_exchange_fanout1', '*'))
    loop.run_forever()
