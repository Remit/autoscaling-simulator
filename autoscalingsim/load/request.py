import uuid
import pandas as pd

from autoscalingsim.utils.request_processing_info import RequestProcessingInfo

class Request:

    """
    Combines static and dynamic information of an individual request/response.
    Since simulator aims at capturing the tail latency bahavior, each request
    matters. The processing times are dynamically updated when the request
    passes through the services. A request becomes a response once its upstream
    flag is dropped.
    """

    def __init__(self, region_name : str, request_type : str, request_processing_info : RequestProcessingInfo, simulation_step : pd.Timedelta, request_id = None):

        self.region_name = region_name
        self.request_type = request_type # TODO: consider removing since this info is in request_requirement
        self.request_processing_info = request_processing_info
        self.simulation_step = simulation_step
        self.request_id = uuid.uuid1() if request_id is None else request_id

        self.processing_time_left = pd.Timedelta(0, unit = 'ms')
        self.waiting_on_link_left = pd.Timedelta(0, unit = 'ms')

        self._cumulative_time = pd.Timedelta(0, unit = 'ms')
        self.network_time = pd.Timedelta(0, unit = 'ms')
        self.buffer_time = dict()

        self._processing_service = None
        self._cur_resource_requirements = None
        self._size = None
        self._upstream = True
        self.replies_expected = 1

    def set_downstream(self):

        self._upstream = False

    def set_upstream(self):

        self._upstream = True

    def set_downstream_processing_time_for_current_service(self):

        self.processing_time_left = self.request_processing_info.get_downstream_processing_time(self.processing_service)

    def set_upstream_processing_time_for_current_service(self):

        self.processing_time_left = self.request_processing_info.get_upstream_processing_time(self.processing_service)

    @property
    def processing_service(self):

        return self._processing_service

    @processing_service.setter
    def processing_service(self, new_processing_service : str):

        self._cur_resource_requirements = self.request_processing_info.resource_requirements.sample
        self._processing_service = new_processing_service
        self._size = self.request_processing_info.request_size.sample if self._upstream else self.request_processing_info.response_size.sample

    @property
    def entry_service(self):

        return self.request_processing_info.entry_service

    @property
    def resource_requirements(self):

        return self._cur_resource_requirements

    @property
    def request_size(self):

        return self._size

    @property
    def response_size(self):

        return self._size

    @property
    def timeout(self):

        return self.request_processing_info.timeout

    @property
    def upstream(self):

        return self._upstream

    @property
    def downstream(self):

        return not self._upstream

    @property
    def cumulative_time(self):

        return self._cumulative_time

    @cumulative_time.setter
    def cumulative_time(self, new_val):

        self._cumulative_time = new_val

    def __deepcopy__(self, memo):

        req_copy = self.__class__(self.region_name, self.request_type, self.request_processing_info, self.simulation_step, self.request_id)

        req_copy.processing_time_left = self.processing_time_left
        req_copy.waiting_on_link_left = self.waiting_on_link_left

        req_copy._cumulative_time = self._cumulative_time
        req_copy.network_time = self.network_time
        req_copy.buffer_time = self.buffer_time.copy()

        req_copy._processing_service = self._processing_service
        req_copy._cur_resource_requirements = self._cur_resource_requirements
        req_copy._size = self._size
        req_copy._upstream = self._upstream
        req_copy.replies_expected = self.replies_expected
        memo[id(req_copy)] = req_copy
        return req_copy

    def __repr__(self):

        return f'{self.__class__.__name__}(region_name = {self.region_name}, request_type = {self.request_type}, \
                                           request_processing_info = {self.request_processing_info}, simulation_step = {self.simulation_step}, \
                                           request_id = {self.request_id})'
