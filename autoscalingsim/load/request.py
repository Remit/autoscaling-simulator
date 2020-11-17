import uuid
import pandas as pd

from ..utils.requirements import ResourceRequirements

class Request:

    """
    Wraps both static and dynamic information of an individual request.
    Since simulator aims at capturing the tail latency bahviour, each request
    matters. The processing times in Request are dynamically updated when
    the request passes through the services that process it.
    """

    def __init__(self,
                 region_name : str,
                 request_type : str,
                 request_id = None):
        # Static state
        self.region_name = region_name
        self.request_type = request_type
        if request_id is None:
            self.request_id = uuid.uuid1()
        else:
            self.request_id = request_id

        # Dynamic state
        self.processing_time_left = pd.Timedelta(0, unit = 'ms')
        self.waiting_on_link_left = pd.Timedelta(0, unit = 'ms')

        self.cumulative_time = pd.Timedelta(0, unit = 'ms')
        self.network_time = pd.Timedelta(0, unit = 'ms')
        self.buffer_time = {}

        self.processing_service = None
        self.upstream = True
        self.replies_expected = 1 # to implement the fan-in on the level of service

    def set_downstream(self):
        self.upstream = False

class RequestProcessingInfo:

    """
    Wraps static information on how request is processed in the application,
    e.g. in which order it passes through services, which amount of time spends
    there being processed, and which amount of resources it consumes at each
    simulation step.
    """

    def __init__(self,
                 request_type : str,
                 entry_service : str,
                 processing_times : dict,
                 timeout : pd.Timedelta,
                 request_size_b : int,
                 response_size_b : int,
                 request_operation_type : str,
                 request_processing_requirements : dict):

        self.request_type = request_type
        self.entry_service = entry_service
        self.processing_times = {}
        for req_type, processing_times_lst in processing_times.items():
            self.processing_times[req_type] = [pd.Timedelta(processing_time_raw_val, unit = 'ms') for processing_time_raw_val in processing_times_lst]
        self.timeout = timeout
        self.request_size_b = request_size_b
        self.response_size_b = response_size_b
        self.request_operation_type = request_operation_type
        self.resource_requirements = ResourceRequirements(request_processing_requirements)
