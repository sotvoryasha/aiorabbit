import os
import yaml
import json
import asyncio
import argparse

from utils import Config
from publisher import Publisher


def callback(message):
    print('CALLBACK :', message.json())


async def acallback(message):
    print('ACALLBACK: ', message.json())


def read_config_from_yml():
    with open('simple_topology_rmq.yml') as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)
    return config_dict


async def main(message_count=100000):
    if not message_count:
        message_count = 100000
    os.environ['RMQ_TOPOLOGY'] = json.dumps(read_config_from_yml())
    conf = Config.create_from_env()
    # consumer1 = Consumer(**conf.__dict__)
    # consumer2 = Consumer(**conf.__dict__)
    publisher = Publisher(**conf.__dict__)
    # await consumer1.run()
    # await consumer2.run()
    await publisher.run()
    # await consumer1.consume(callback, 'test_queue1')
    # await consumer2.consume(acallback, 'test_queue2')
    for i in range(message_count):
        await publisher.send_message({1: i}, 'test_exchange_fanout1', '*')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--message_count', type=int, required=False, help='Колличество сообщений')
    args = parser.parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args.message_count))
    loop.run_forever()
    loop.close()
