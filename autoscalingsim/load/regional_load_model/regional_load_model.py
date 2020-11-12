import pandas as pd
from abc import ABC, abstractmethod

class RegionalLoadModel(ABC):

    """
    An interface for different kinds of load models (per region)
    """

    _Registry = {}

    @classmethod
    def register(cls,
                 name : str):

        def decorator(regional_load_model_class):
            cls._Registry[name] = regional_load_model_class
            return regional_load_model_class

        return decorator

    @classmethod
    def get(cls,
            name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent regional load model {name}')

        return cls._Registry[name]

    @abstractmethod
    def __init__(self,
                 region_name : str,
                 pattern : dict,
                 load_configs : dict,
                 simulation_step : pd.Timedelta):

        pass

    @abstractmethod
    def generate_requests(self,
                          timestamp : pd.Timestamp):
        pass

from .realizations import *
