from abc import ABC, abstractmethod


class PlacingStrategy(ABC):

    _Registry = {}

    @abstractmethod
    def place(self):

        pass

    @classmethod
    def register(cls, name : str):

        def decorator(placing_strategy_class):
            cls._Registry[name] = placing_strategy_class
            return placing_strategy_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

    @classmethod
    def items(cls):

        return cls._Registry.copy().items()

from .placing_strategy_impl import *
