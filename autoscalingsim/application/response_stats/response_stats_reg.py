import pandas as pd

from autoscalingsim.load.request import Request
from autoscalingsim.utils.metric_source import MetricSource
from .response_stats import ResponseStats

class ResponseStatsRegionalized(MetricSource):

    def __init__(self, regions : list):

        self.regional_stats = { region_name : ResponseStats() for region_name in regions }

    def add_request(self, cur_timestamp : pd.Timestamp, req : Request):

        self.regional_stats[req.region_name].add_request(cur_timestamp, req)

    def get_response_times_by_request(self):

        return { region_name : reg_stats.joint_untimed_stats(ResponseStats.KEY_RESPONSE_TIME) for region_name, reg_stats in self.regional_stats.items() }

    def get_network_times_by_request(self):

        return { region_name : reg_stats.joint_untimed_stats(ResponseStats.KEY_NETWORK_TIME) for region_name, reg_stats in self.regional_stats.items() }

    def get_buffer_times_by_request(self):

        return { region_name : reg_stats.joint_untimed_stats(ResponseStats.KEY_BUFFER_TIME) for region_name, reg_stats in self.regional_stats.items() }

    def get_metric_value(self, region_name : str, metric_name : str, submetric_name : str):

        if not region_name in self.regional_stats:
            raise ValueError(f'Unknown region name {region_name}')

        return self.regional_stats[region_name].timed_stats(metric_name, submetric_name)

    def get_aspect_value(self, region_name : str, aspect_name : str):

        raise NotImplementedError(f'Not supported for {self.__class__.__name__}')

    def get_resource_requirements(self, region_name : str):

        raise NotImplementedError(f'Not supported for {self.__class__.__name__}')

    def get_placement_parameter(self, region_name : str, parameter : str):

        raise NotImplementedError(f'Not supported for {self.__class__.__name__}')
