import pandas as pd

from abc import ABC, abstractmethod

class MetricConverter(ABC):

    _Registry = {}
    DEFAULT_CONVERTER_NAME = 'noop'

    @abstractmethod
    def __init__(self, metric_params : dict):

        pass

    @abstractmethod
    def convert_df(self, df : pd.DataFrame) -> pd.DataFrame:

        pass

    @classmethod
    def register(cls, name : str):

        def decorator(metric_converter_class):
            cls._Registry[name] = metric_converter_class
            return metric_converter_class

        return decorator

    @classmethod
    def get(cls, name : str):

        return cls._Registry[name] if name in cls._Registry else cls._Registry[cls.DEFAULT_CONVERTER_NAME]

    @classmethod
    def knows(cls, name : str):

        return name in cls._Registry

from .converters_realizations import *
