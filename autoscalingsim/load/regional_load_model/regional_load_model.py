import pandas as pd
from abc import ABC, abstractmethod

class RegionalLoadModel(ABC):

    """
    An interface for different kinds of load models (per region)
    """

    ALL_REQUEST_TYPES_WILDCARD = '*'
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

    def get_requests_count_per_unit_of_time(self, req_type : str,
                                            averaging_interval : pd.Timedelta = pd.Timedelta(10, unit = 'ms')):

        req_types_to_consider = [req_type]
        if req_type == self.__class__.ALL_REQUEST_TYPES_WILDCARD:
            req_types_to_consider = self.load.keys()
        elif not req_type in self.load:
            raise ValueError(f'No request of type {req_type} found in the load stats for region {self.region_name}')

        request_counts = pd.DataFrame(columns = ['value'], index = pd.to_datetime([]))

        for req_type in req_types_to_consider:
            cur_request_counts = pd.DataFrame(self.load[req_type]).set_index('datetime')
            # Aligning the time series
            common_index = cur_request_counts.index.union(request_counts.index)#.astype(cur_request_counts.index.dtype)
            cur_request_counts = cur_request_counts.reindex(common_index, fill_value = 0)
            request_counts = request_counts.reindex(common_index, fill_value = 0)
            request_counts += cur_request_counts

        return request_counts.rolling(averaging_interval).mean()

from .load_models import *
