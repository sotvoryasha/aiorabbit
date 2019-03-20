import json

from logger import logger
from client import RMQClient


class Publisher(RMQClient):
    async def send_message(self, payload, exchange, routing_key):
        if isinstance(payload, dict):
            payload = json.dumps(payload).encode()
        elif not isinstance(payload, bytes):
            try:
                payload = str(payload).encode()
            except Exception as e:
                logger.error(e)
                raise ValueError("Can not convert payload to bytes")

        await RMQClient.publish_await(self, payload, exchange, routing_key)
