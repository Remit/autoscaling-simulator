import pandas as pd

from autoscalingsim.load.request import Request
from .response_quality_stats import ResponseQualityStats

class BufferTimeStats(ResponseQualityStats):

    def add_request(self, cur_timestamp : pd.Timestamp, req : Request):

        for service_name, buffer_time_val in req.buffer_time.items():
            self._add_request_stats(cur_timestamp, buffer_time_val.microseconds / 1000, [req.request_type, service_name])
