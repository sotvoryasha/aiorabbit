import asyncio
import aioamqp
import itertools
import time

from logger import logger
from rmq_declaring import BindingType, RExchanges, RQueues


class RMQClient:
    """
    Класс для описание клиента rabbitMQ
    """
    def __init__(self,
                 host: str,
                 port: int,
                 login: str,
                 password: str,
                 virtualhost: str,
                 exchanges_settings: RExchanges,
                 queues_settings: RQueues,
                 reconnect_backoff=30,
                 retries_to_notify=-1,
                 max_retries=-1):

        self._host = host
        self._port = port
        self._login = login
        self._password = password
        self._virtualhost = virtualhost
        self._reconnect_backoff = reconnect_backoff

        self.protocol = None
        self.transport = None
        self.channel = None
        self.queues_settings = queues_settings
        self.exchanges_settings = exchanges_settings

        self.exchanges = {}
        self.queues = {}

        self.structure_setting = None
        self.default_reconnect_action = None
        self.max_retries = max_retries
        self.is_infinite_reconnect = self.max_retries == -1
        self.retries_to_notify = retries_to_notify

    async def close(self):
        try:
            self.channel.close()
        except BaseException as e:
            logger.error(e)
        try:
            self.protocol.close()
        except BaseException as e:
            logger.error(e)
        try:
            self.transport.close()
        except BaseException as e:
            logger.error(e)

    async def on_error(self):
        await self.reconnect()

    @property
    def connection_data(self):
        return {
            'host': self._host,
            'port': self._port,
            'login': self._login,
            'password': self._password,
            'virtualhost': self._virtualhost
        }

    async def connect(self):
        """
        Подключение к сереверу rabbitMQ и инициализация exchange и queue

        Returns
        -------

        """
        try:
            self.transport, self.protocol = await aioamqp.connect(**self.connection_data)
            self.channel = await self.protocol.channel()
        except aioamqp.AioamqpException:
            logger.warning('RabbitMQ connection error')
            self.transport, self.protocol = None, None

    async def reconnect(self):
        """
        Попытки переподключиться

        Returns
        -------

        """
        reconnect_count = 0
        # TODO: переделать колхоз
        while True:
            if self.channel is None or not self.channel.is_open:
                logger.info('Connection to rabbitmq')

                try:
                    await self.connect()
                    await self.initialize_exchanges_and_queues()
                    logger.info('Client successfully connected')
                    reconnect_count = 0
                except (ConnectionError, OSError, aioamqp.AioamqpException):
                    self.transport, self.protocol, self.channel = None, None, None
                    logger.warn(f'Try to connect to rabbitmq is failed. '
                                f'Will retries after {self._reconnect_backoff} seconds')
                    time.sleep(self._reconnect_backoff)
                    reconnect_count += 1
            await asyncio.sleep(3)
            if self.retries_to_notify == reconnect_count:
                logger.error('Maximum number of retries reached.')

    async def initialize_exchanges_and_queues(self):
        if self.channel and self.channel.is_open:
            for exchange_setting in self.exchanges_settings:
                exchange = await self.channel.exchange_declare(**exchange_setting.declare_kwargs)
                self.exchanges[exchange_setting.declare_kwargs['exchange_name']] = exchange

            for queue_setting in self.queues_settings:
                queue = await self.channel.queue_declare(**queue_setting.declare_kwargs)
                self.queues[queue_setting.declare_kwargs['queue_name']] = queue

            await self.set_bindings()

    async def set_bindings(self):
        # setting bindings
        for setting in itertools.chain(self.exchanges_settings, self.queues_settings):
            bindings = setting.binding_data or []
            for bind in bindings:
                if bind.binding_type == BindingType.EE:
                    await self.channel.exchange_bind(*bind.as_args)
                else:
                    await self.channel.queue_bind(*bind.as_args)

    async def run(self):
        loop = asyncio.get_event_loop()
        await loop.create_task(self.reconnect())