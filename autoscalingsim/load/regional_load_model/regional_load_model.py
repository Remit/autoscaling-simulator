import pandas as pd
from abc import ABC, abstractmethod

class RegionalLoadModel(ABC):

    """
    An interface for different kinds of load models (per region)
    """

    _Registry = {}

    @classmethod
    def register(cls, name : str):

        def decorator(regional_load_model_class):
            cls._Registry[name] = regional_load_model_class
            return regional_load_model_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent regional load model: {name}')

        return cls._Registry[name]

    @abstractmethod
    def __init__(self, region_name : str, pattern : dict, load_configs : dict,
                 simulation_step : pd.Timedelta):

        pass

    @abstractmethod
    def generate_requests(self, timestamp : pd.Timestamp):

        pass

    def get_stat(self):

        return { req_type : pd.DataFrame(dict_load).set_index('datetime') for req_type, dict_load in self.load.items() }

    def _update_stat(self, timestamp : pd.Timestamp, req_type : str, reqs_num : int):

        """ Stat is stored as dicts to improve the performance that suffers when using dataframes frequently """

        if req_type in self.load:
            self.load[req_type]['datetime'].append(timestamp)
            self.load[req_type]['value'].append(reqs_num)
        else:
            self.load[req_type] = {'datetime': [timestamp], 'value': [reqs_num]}

from .load_models import *
