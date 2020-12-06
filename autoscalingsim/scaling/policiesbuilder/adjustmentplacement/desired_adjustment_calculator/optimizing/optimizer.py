from abc import ABC, abstractmethod

class Optimizer(ABC):

    """ Attempts to find the best placement given the results of the scoring """

    _Registry = {}

    @abstractmethod
    def select_best(self, scored_options : dict):

        pass

    @classmethod
    def register(cls, name : str):

        def decorator(optimizer_class):
            cls._Registry[name] = optimizer_class
            return optimizer_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

from .optimizer_impl import *
