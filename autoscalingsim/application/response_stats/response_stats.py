import pandas as pd

from autoscalingsim.load.request import Request
from .response_quality_stats import *

class ResponseStats:

    KEY_RESPONSE_TIME = 'response_time'
    KEY_NETWORK_TIME = 'network_time'
    KEY_BUFFER_TIME = 'buffer_time'

    def __init__(self, requests_types):

        self.stats = {}
        self.stats[self.__class__.KEY_RESPONSE_TIME] = ResponseTimeStats(requests_types)
        self.stats[self.__class__.KEY_NETWORK_TIME] = NetworkTimeStats(requests_types)
        self.stats[self.__class__.KEY_BUFFER_TIME] = BufferTimeStats(requests_types)

    def add_request(self, cur_timestamp : pd.Timestamp, req : Request):

        for stats in self.stats.values():
            stats.add_request(cur_timestamp, req)

    def joint_untimed_stats(self, stats_kind : str):

        return self.stats[stats_kind].joint_untimed_stats()

    def timed_stats(self, stats_kind : str, request_type : str):

        return self.stats[stats_kind].timed_stats_for_request_type(request_type)
