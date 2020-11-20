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

    def __init__(self, region_name : str, request_type : str, request_id = None):

        self.region_name = region_name
        self.request_type = request_type
        self.request_id = uuid.uuid1() if request_id is None else request_id

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
