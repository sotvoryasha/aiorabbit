import yaml
import asyncio
from rmqclient import RMQClient, Config


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    config = None
    with open('simple_topology_rmq.yml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    conf = Config(config)
    client = RMQClient(**conf.__dict__)
    print(config)
    loop.run_until_complete(client.run())
    loop.run_until_complete(client.channel.basic_publish(payload='Hello', exchange_name='test_exchange_fanout1', routing_key='*'))
    loop.run_forever()
