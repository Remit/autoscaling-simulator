import collections
import pandas as pd

from autoscalingsim.load.request import Request
from .response_quality_stats import ResponseQualityStats
from autoscalingsim.utils.timeline import Timeline

class ResponseTimeStats (ResponseQualityStats):

    def __init__(self):

        self.metric_by_request = collections.defaultdict(Timeline)

    def add_request(self, cur_timestamp : pd.Timestamp, req : Request):

        for i in range(req.batch_size):
            self._add_request_stats(cur_timestamp, req.cumulative_time.value / 1_000_000, [req.request_type])
