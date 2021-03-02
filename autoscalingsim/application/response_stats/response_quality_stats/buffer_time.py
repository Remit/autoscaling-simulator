import collections
import pandas as pd

from autoscalingsim.load.request import Request
from .response_quality_stats import ResponseQualityStats
from autoscalingsim.utils.timeline import Timeline

class BufferTimeStats(ResponseQualityStats):

    def __init__(self):

        self.metric_by_request = collections.defaultdict(lambda: collections.defaultdict(Timeline))

    def add_request(self, cur_timestamp : pd.Timestamp, req : Request):

        for service_name, buffer_time_val in req.buffer_time.items():
            for i in range(req.batch_size):
                self._add_request_stats(cur_timestamp, buffer_time_val.value / 1_000_000, [req.request_type, service_name])
