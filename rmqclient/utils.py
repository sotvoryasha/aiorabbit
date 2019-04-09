import os
import json


from rmq_declaring import RExchanges, RQueues, DataBindingFields, ExchangeSettingsFields, QueueSettingsFields, \
    BindingType


class Config:
    host: str = 'localhost'
    port: int = 5672
    login: str = 'guest'
    password: str = 'guest'
    virtualhost: str = '/'
    queues_settings: RQueues = None
    exchanges_settings: RExchanges = None

    def __init__(self, host=None, port=None, login=None, password=None, virtualhost=None, config_topology=None):
        if not config_topology:
            raise ValueError('Empty config_topology')

        self.host = host or Config.host
        self.port = port or Config.port
        self.login = login or Config.password
        self.password = password or Config.password
        self.virtualhost = virtualhost or Config.virtualhost

        if not config_topology.get('exchanges') or not config_topology.get('queues'):
            raise ValueError('Exchanges and Queues is not set.')

        self.queues_settings = convert_queues_from_config(config_topology.get('queues'))
        self.exchanges_settings = convert_exchanges_from_config(config_topology.get('exchanges'))

    @classmethod
    def create_from_env(cls):
        """
        Создаёт Config из переменных окружения

        Returns
        -------
        Config
        """
        credential_dict = get_credential_from_env()
        host = credential_dict.get('host')
        port = credential_dict.get('port')
        login = credential_dict.get('login')
        password = credential_dict.get('password')
        virtualhost = credential_dict.get('virtualhost')
        config_topology = json.loads(credential_dict.get('topology'))
        return cls(host, port, login, password, virtualhost, config_topology)


def convert_binding_data(setting, entity_type='queue')-> list or None:
    """
    Конвертирует параметры для биндинга
    Parameters
    ----------
    setting
        dict
    entity_type
        str: exchange or queue
    Returns
    -------
    list or None
    """

    converted_data = None
    bindings = setting.pop('binding_data', [])
    if entity_type == 'exchange':
        converted_data = [DataBindingFields(**b, destination=setting.get('exchange_name'), binding_type=BindingType.EE)
                          for b in bindings]

    if entity_type == 'queue':
        converted_data = [DataBindingFields(**b, destination=setting.get('queue_name'), binding_type=BindingType.EQ)
                          for b in bindings]

    return converted_data


def convert_exchanges_from_config(exchanges_configs: dict)->list:
    """
    Функция для конвертирования конфига в сущности rmq_declaring

    Parameters
    ----------

    exchanges_configs
        [{
                exchange_name: str (required),
                type_name: str (default: 'fanout),
                binding_data: [{
                    source: str (required),
                    routing_key: str,
                }] (not required),
                durable: bool (default: True)
                auto_delete: bool (default: False)
                passive: bool (default: False)
                no_wait: bool (default: False)
                arguments: dict (default: None)
            }]


    Для ознакомления с параметрами и что значат,
    смотреть в библиотеки aioamqp файл channel.py метод: declare_exchange

    Returns
    -------
    list

    """

    exchanges = []
    for exchange_setting in exchanges_configs:
        binding_data = convert_binding_data(exchange_setting, 'exchange')
        exchange = ExchangeSettingsFields(binding_data=binding_data, **exchange_setting)
        exchanges.append(exchange)
    return exchanges


def convert_queues_from_config(queues_configs: dict)->list:
    """

    Parameters
    ----------
    queues_configs
        [{
            queue_name: str
            binding_data: [{
                source: str (required),
                routing_key: str
            }] (required),
            durable: bool (default: True)
            exclusive: bool (default: False)
            auto_delete: bool (default: False)
            passive: bool (default: False)
            no_wait: bool (default: False)
            arguments: dict (default: None)
        }]
    Для ознакомления с параметрами и что значат,
    смотреть в библиотеки aioamqp файл channel.py метод: declare_queue

    Returns
    -------

    """

    queues = []
    for queue_setting in queues_configs:
        binding_data = convert_binding_data(queue_setting, 'queue')
        queue = QueueSettingsFields(binding_data=binding_data, **queue_setting)
        queues.append(queue)
    return queues


def get_credential_from_env()->dict:
    """

    Returns
    -------
        dict
            host: str (default: 127.0.0.1),
            port: int (default: 5672),
            login: str (default: guest),
            password: str (default: guest),
            virtualhost: str (default: '')

    """

    return {'host': os.getenv('RMQ_HOST', ''),
            'port': os.getenv('RMQ_PORT', None),
            'login': os.getenv('RMQ_LOGIN', ''),
            'password': os.getenv('RMQ_PASSWORD', ''),
            'virtualhost': os.getenv('RMQ_VHOST', '/'),
            'topology': os.getenv('RMQ_TOPOLOGY', {})}
