import asyncio

from client import RMQClient
from rmq_declaring import RMQMessage


def sync_to_async(func):
    async def wrap_sync(*args, **kwargs):
        return func(*args, **kwargs)
    return wrap_sync


class Consumer(RMQClient):
    on_message = None

    async def message_creator_callback(self, *args):
        message = RMQMessage(*args)
        try:
            result = self.on_message(message)
            if asyncio.iscoroutine(result):
                await result
        except:
            # TODO: sentry
            await message.reject()

    async def consume_await(self, task, queue_name):
        self.on_message = task
        asyncio.ensure_future(RMQClient.consume_await(self, self.message_creator_callback, queue_name))

