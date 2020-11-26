import pandas as pd

from autoscalingsim.load.request import Request
from autoscalingsim.utils.metric_source import MetricSource

class ResponseStatsRegional:

    """
    Load statistics collected for a particular region.
    """

    DEFAULT_REQUEST_TYPE = '*'

    def __init__(self):

        self.response_times_by_request = {}
        self.network_times_by_request = {}
        self.buffer_times_by_request = {}

    def add_request(self, cur_timestamp : pd.Timestamp, req : Request):

        # Response received by the user
        if not req.request_type in self.response_times_by_request:
            self.response_times_by_request[req.request_type] = {}
            if not cur_timestamp in self.response_times_by_request[req.request_type]:
                self.response_times_by_request[req.request_type][cur_timestamp] = list()
            self.response_times_by_request[req.request_type][cur_timestamp].append(req.cumulative_time.microseconds / 1000)

        # Time spent transferring between the nodes
        if not req.request_type in self.network_times_by_request:
            self.network_times_by_request[req.request_type] = {}
            if not cur_timestamp in self.network_times_by_request[req.request_type]:
                self.network_times_by_request[req.request_type][cur_timestamp] = list()
            self.network_times_by_request[req.request_type][cur_timestamp].append(req.network_time.microseconds / 1000)

        # Time spent waiting in the buffers
        if len(req.buffer_time) > 0:
            if not req.request_type in self.buffer_times_by_request:
                self.buffer_times_by_request[req.request_type] = {}

            for service_name, buffer_time_val in req.buffer_time.items():
                if not service_name in self.buffer_times_by_request[req.request_type]:
                    self.buffer_times_by_request[req.request_type][service_name] = {}
                    if not cur_timestamp in self.buffer_times_by_request[req.request_type][service_name]:
                        self.buffer_times_by_request[req.request_type][service_name][cur_timestamp] = list()
                    self.buffer_times_by_request[req.request_type][service_name][cur_timestamp].append(buffer_time_val.microseconds / 1000)

    def get_response_times_by_request_flat(self):

        result = {}
        for req_type, timeline in self.response_times_by_request.items():
            if not req_type in result:
                result[req_type] = []
            for ts, vals_lst in timeline.items():
                result[req_type].extend(vals_lst)

        return result

    def get_network_times_by_request_flat(self):

        result = {}
        for req_type, timeline in self.network_times_by_request.items():
            if not req_type in result:
                result[req_type] = []
            for ts, vals_lst in timeline.items():
                result[req_type].extend(vals_lst)

        return result

    def get_buffer_times_by_request_flat(self):

        result = {}
        for req_type, timeline_per_service in self.buffer_times_by_request.items():
            if not req_type in result:
                result[req_type] = []
            for service_name, timeline in timeline_per_service.items():
                result[req_type] = { service_name : [] }
                for ts, vals_lst in timeline.items():
                    result[req_type][service_name].extend(vals_lst)

        return result

    def get_metric_value(self, metric_name : str, request_type : str):

        result_raw = {}
        if metric_name == 'response_time':

            request_types_to_consider = [request_type]
            if request_type == self.__class__.DEFAULT_REQUEST_TYPE:
                request_types_to_consider = self.response_times_by_request.keys()

            for request_type in request_types_to_consider:
                for ts, vals_lst in self.response_times_by_request[request_type].items():
                    if not ts in result_raw:
                        result_raw[ts] = []
                    result_raw[ts].extend(vals_lst)

        elif metric_name == 'network_time':

            request_types_to_consider = [request_type]
            if request_type == self.__class__.DEFAULT_REQUEST_TYPE:
                request_types_to_consider = self.network_times_by_request.keys()

            for request_type in request_types_to_consider:
                for ts, vals_lst in self.network_times_by_request[request_type].items():
                    if not ts in result_raw:
                        result_raw[ts] = []
                    result_raw[ts].extend(vals_lst)

        elif metric_name == 'buffer_time':

            request_types_to_consider = [request_type]
            if request_type == self.__class__.DEFAULT_REQUEST_TYPE:
                request_types_to_consider = self.buffer_times_by_request.keys()

            for request_type in request_types_to_consider:
                for service_name, timeline in self.buffer_times_by_request[request_type].items():
                    for ts, vals_lst in timeline.items():
                        if not ts in result_raw:
                            result_raw[ts] = []
                        result_raw[ts].extend(vals_lst)

        result = { ts : self._mean_val(vals_lst) for ts, vals_lst in result_raw.items() }
        return pd.DataFrame({'value': list(result.values())}, index = pd.to_datetime(list(result.keys())))

    def _mean_val(self, vals_lst : list):

        return sum(vals_lst) / len(vals_lst) if len(vals_lst) > 0 else 0

class ResponseStats(MetricSource):

    def __init__(self):
        self.regional_stats = {}

    def add_request(self, cur_timestamp : pd.Timestamp, req : Request):

        if not req.region_name in self.regional_stats:
            self.regional_stats[req.region_name] = ResponseStatsRegional()

        self.regional_stats[req.region_name].add_request(cur_timestamp, req)

    def get_response_times_by_request(self):

        return { region_name : reg_stats.get_response_times_by_request_flat() for region_name, reg_stats in self.regional_stats.items() }

    def get_network_times_by_request(self):

        return { region_name : reg_stats.get_network_times_by_request_flat() for region_name, reg_stats in self.regional_stats.items() }

    def get_buffer_times_by_request(self):

        return { region_name : reg_stats.get_buffer_times_by_request_flat() for region_name, reg_stats in self.regional_stats.items() }

    def get_metric_value(self, region_name : str, metric_name : str, submetric_name : str):

        if not region_name in self.regional_stats:
            raise ValueError(f'Unknown region name {region_name}')

        return self.regional_stats[region_name].get_metric_value(metric_name, submetric_name)

    def get_aspect_value(self, region_name : str, aspect_name : str):

        raise NotImplementedError(f'Not supported for {self.__class__.__name__}')

    def get_resource_requirements(self, region_name : str):

        raise NotImplementedError(f'Not supported for {self.__class__.__name__}')

    def get_placement_parameter(self, region_name : str, parameter : str):

        raise NotImplementedError(f'Not supported for {self.__class__.__name__}')
