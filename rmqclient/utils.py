import os

from rmq_declaring import RExchanges, RQueues, DataBindingFields, ExchangeSettingsFields, QueueSettingsFields, \
    BindingType


class Config:
    host: str = '127.0.0.1'
    port: int = 5672
    login: str = 'guest'
    password: str = 'guest'
    virtualhost: str = ''
    queues_settings: RQueues = None
    exchanges_settings: RExchanges = None

    def __init__(self, config_topology: dict):
        credential_dict = get_credential_from_env()
        self.host = credential_dict.get('host')
        self.port = credential_dict.get('port')
        self.login = credential_dict.get('login')
        self.password = credential_dict.get('password')
        self.virtualhost = credential_dict.get('virtualhost')

        if not config_topology.get('exchanges') or not config_topology.get('queues'):
            raise ValueError('Exchanges and Queues is not set.')

        self.exchanges_settings = convert_exchanges_from_config(config_topology.get('exchanges'))
        self.queues_settings = convert_queues_from_config(config_topology.get('queues'))


def convert_binding_data(setting, entity_type='queue'):
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

    """

    converted_data = None
    bindings = setting.pop('binding_data', [])
    if entity_type == 'exchange':
        converted_data = [DataBindingFields(**bind,
                                            destination=setting.get('exchange_name'),
                                            binding_type=BindingType.EE)
                          for bind in bindings]
    if entity_type == 'queue':
        converted_data = [DataBindingFields(**bind,
                                            destination=setting.get('queue_name'),
                                            binding_type=BindingType.EQ)
                          for bind in bindings]

    return converted_data


def convert_exchanges_from_config(exchanges_configs: dict):
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
    Config

    """

    exchanges = []
    for exchange_setting in exchanges_configs:
        binding_data = convert_binding_data(exchange_setting, 'exchange')
        exchange = ExchangeSettingsFields(binding_data=binding_data, **exchange_setting)
        exchanges.append(exchange)
    return exchanges


def convert_queues_from_config(queues_configs):
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

    return {'host': os.getenv('host', '127.0.0.1'),
            'port': os.getenv('port', 5672),
            'login': os.getenv('login', 'guest'),
            'password': os.getenv('password', 'guest'),
            'virtualhost': os.getenv('virtualhost', '/')}
