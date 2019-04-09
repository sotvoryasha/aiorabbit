import os
import asyncio
import aioamqp

from logger import logger
from rmq_declaring import RExchanges, RQueues


class RMQClient:
    instance_type = 'Client'
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

        self._loop = asyncio.get_event_loop()

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

    def create_from_env(self):
        host = os.getenv('RABBIT_HOST')
        port = os.getenv('RABBIT_PORT')

    async def close(self):
        """
        Закрываем клиент корректно.

        Returns
        -------

        """

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

    @property
    def connection_data(self):
        """
        Возвращаем словарь с данными для подключения к серверу
        Returns
        -------

        """

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
        None
        """
        try:
            self.transport, self.protocol = await aioamqp.connect(**self.connection_data)
            self.channel = await self.protocol.channel()
        except aioamqp.AioamqpException:
            logger.warning(f'{self.instance_type} RabbitMQ connection error ')
            self.transport, self.protocol = None, None

    async def continuous_connection(self):
        while True:
            if self.channel is None or not self.channel.is_open:
                await self.reconnect()
            await asyncio.sleep(1)

    async def reconnect(self):
        """
        Попытки переподключиться

        Returns
        -------

        """

        logger.info('Connection to rabbitmq')
        try:
            await self.connect()
            await self.initialize_exchanges_and_queues()
            logger.info(f'{self.instance_type} successfully connected')
        except (ConnectionError, OSError, aioamqp.AioamqpException):
            self.transport = self.protocol = self.channel = None
            logger.warn(f'{self.instance_type} Try to connect to rabbitmq is failed. '
                        f'Will retries after {self._reconnect_backoff} seconds')
            await asyncio.sleep(self._reconnect_backoff)

    async def initialize_exchanges_and_queues(self):
        """
        Инциализация схемы точек входа и очередей
        Если точка входа или очередь не была создана, то создаёт
        ВНИМАНИЕ: если очередь или точка входа существует на сервере RMQ, то переопределить её нельзя
        Returns
        -------
        None
        """

        if self.channel and self.channel.is_open:
            for exchange_setting in self.exchanges_settings:
                exchange = await self.channel.exchange_declare(**exchange_setting.declare_kwargs)
                self.exchanges[exchange_setting.declare_kwargs['exchange_name']] = exchange

            for queue_setting in self.queues_settings:
                queue = await self.channel.queue_declare(**queue_setting.declare_kwargs)
                self.queues[queue_setting.declare_kwargs['queue_name']] = queue

            await self.set_bindings()

    async def set_bindings(self):
        """
        Метод связки очередей и точек входа

        Returns
        -------

        """

        for setting in self.exchanges_settings:
            bindings = setting.binding_data or []
            for bind in bindings:
                await self.channel.exchange_bind(*bind.as_args)

        for setting in self.queues_settings:
            bindings = setting.binding_data or []
            for bind in bindings:
                await self.channel.queue_bind(*bind.as_args)

    async def run(self):
        """
        Метод для запуска клиента
        пример запуска:
        client = RMQClient(config)
        await client.run()

        Returns
        -------

        """

        self._loop = asyncio.get_event_loop()
        return asyncio.ensure_future(self.continuous_connection(), loop=self._loop)

    async def publish(self, payload, exchange_name, routing_key, properties=None):
        """
        Отправка сообщения в exchange

        Parameters
        ----------
        payload
        exchange_name
        routing_key
        properties

        Returns
        -------
        None
        """

        while not self.channel:
            await asyncio.sleep(1)
        await self.channel.basic_publish(payload=payload,
                                         exchange_name=exchange_name,
                                         routing_key=routing_key,
                                         properties=properties)

    async def consume(self, callback, queue_name, no_ack=False, **kwargs):
        """
        Подписаться на очередь

        Parameters
        ----------
        callback
        queue_name
        no_ack
        kwargs

        Returns
        -------
        None
        """

        while not self.channel:
            await asyncio.sleep(1)
        await self.channel.basic_consume(callback=callback, queue_name=queue_name, no_ack=no_ack, **kwargs)
