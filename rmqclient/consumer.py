import asyncio

from client import RMQClient
from rmq_declaring import RMQMessage


def sync_to_async(func):
    async def wrap_sync(*args, **kwargs):
        return func(*args, **kwargs)
    return wrap_sync


class Consumer(RMQClient):
    instance_type = 'Consumer'
    on_message = None

    async def message_creator_callback(self, *args):
        message = RMQMessage(*args)
        try:
            result = self.on_message(message)
            if asyncio.iscoroutine(result):
                await result
            await message.ack()
        except Exception as e:
            # TODO: sentry
            await message.reject()

    async def consume(self, task, queue_name, no_ack=False):
        self.on_message = task
        return asyncio.create_task(RMQClient.consume(self, self.message_creator_callback, queue_name))

