import pandas as pd

from abc import ABC, abstractmethod

from autoscalingsim.load.request import Request
from autoscalingsim.utils.timeline import Timeline

class ResponseQualityStats(ABC):

    DEFAULT_REQUEST_TYPE = '*'

    @abstractmethod
    def add_request(self, cur_timestamp : pd.Timestamp, req : Request):

        pass

    def __init__(self, requests_types):

        self.metric_by_request = { req_type : {} for req_type in requests_types }

    def joint_untimed_stats(self):

        result = {}
        cur_result_level = result
        cur_source_level = self.metric_by_request
        self._modify_untimed_stats_on_level(cur_result_level, cur_source_level)

        return result

    def timed_stats_for_request_type(self, request_type : str):

        request_types_to_consider = [request_type]
        if request_type == self.__class__.DEFAULT_REQUEST_TYPE:
            request_types_to_consider = self.metric_by_request.keys()

        result_raw = {}
        for request_type in request_types_to_consider:
            cur_level = self.metric_by_request[request_type]
            result_for_req_type = self._extract_timeline_as_dict(cur_level)
            for ts, vals_lst in result_for_req_type.items():
                if not ts in result_raw:
                    result_raw[ts] = []
                result_raw[ts].extend(vals_lst)

        result = { ts : self._mean_val(vals_lst) for ts, vals_lst in result_raw.items() }
        return pd.DataFrame({ 'value': list(result.values()) }, index = pd.to_datetime(list(result.keys())))

    def _add_request_stats(self, cur_timestamp : pd.Timestamp, value : float, levels : list):

        """
        Generalized addition of the quality stats for the request, traverses
        all the *levels* of the data structure to store the value according to
        the hierarchy of data.
        """

        cur_level = self.metric_by_request
        for level in levels[:-1]:
            if not level in cur_level:
                cur_level[level] = {}
            cur_level = cur_level[level]

        if not isinstance(cur_level, Timeline):
            cur_level[levels[-1]] = Timeline()
        cur_level[levels[-1]].append_at_timestamp(cur_timestamp, value)

    def _mean_val(self, vals_lst : list):

        return sum(vals_lst) / len(vals_lst) if len(vals_lst) > 0 else 0

    def _modify_untimed_stats_on_level(self, cur_result_level : dict, cur_source_level : dict):

        for k, val in cur_source_level.items():
            if isinstance(val, Timeline):
                cur_result_level[k] = val.get_flattened()
            else:
                cur_result_level[k] = {} if len(val) > 0 else []
                self._modify_untimed_stats_on_level(cur_result_level[k], val)

    def _extract_timeline_as_dict(self, cur_level : dict):

        if isinstance(cur_level, Timeline):
            return cur_level.to_dict()
        else:
            result = {}
            for k, val in cur_level.items():
                timeline_as_dict = self._extract_timeline_as_dict(val)
                for timestamp, val in timeline_as_dict.items():
                    if not timestamp in result:
                        result[timestamp] = []
                    result[timestamp].extend(val)

            return result
