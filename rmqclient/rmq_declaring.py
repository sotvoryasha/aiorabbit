from enum import Enum
from typing import Sequence
from dataclasses import dataclass


def dict_from_fields_name(obj, fields) -> dict:
    """
    Метод для составления словаря по объекту и заданным полям
    Если поле не существует в объекте, то будет вызвана ошибка
    Parameters
    ----------
    obj
        Object
    fields
        iterable object of str
    Returns
    -------
        dict
    """
    d = {}
    for field in fields:
        if hasattr(obj, field):
            d[field] = getattr(obj, field)
        else:
            raise KeyError(f'Object {repr(obj)} does has not have field: {field}')
    return d


class BindingType(Enum):
    """
    Типы связок, точка-точка или точка-очередь

    """
    EE = 'exchange-exchange'
    EQ = 'exchange-queue'


@dataclass
class DataBindingFields:
    """
    Класс описание настроек для связывания

    """
    declaring_fields = ('destination', 'source', 'routing_key')
    source: str
    destination: str
    routing_key: str = '*'
    binding_type: BindingType = BindingType.EQ

    @property
    def as_args(self):
        return dict_from_fields_name(self, self.declaring_fields).values()


DataBindings = Sequence[DataBindingFields]


@dataclass
class ExchangeSettingsFields:
    """
    Класс описание настроек Exchange

    """
    _declaring_fields = ('exchange_name', 'type_name', 'durable', 'auto_delete', 'passive', 'no_wait', 'arguments')
    exchange_name: str
    type_name: str = 'fanout'
    durable: bool = True
    auto_delete: bool = False
    passive: bool = False
    no_wait: bool = False
    arguments = None
    binding_data: DataBindings = None

    @property
    def declare_kwargs(self):
        return dict_from_fields_name(self, self._declaring_fields)


@dataclass
class QueueSettingsFields:
    """
    Класс описание настроек Queue


    """
    _declaring_fields = ('queue_name', 'durable', 'auto_delete', 'passive', 'no_wait', 'arguments')
    queue_name: str
    durable: bool = True
    exclusive: bool = False
    auto_delete: bool = False
    passive: bool = False
    no_wait: bool = False
    arguments = None
    binding_data: DataBindings = None

    @property
    def declare_kwargs(self):
        return dict_from_fields_name(self, self._declaring_fields)


RQueues = Sequence[QueueSettingsFields]
RExchanges = Sequence[ExchangeSettingsFields]
