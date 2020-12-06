import uuid
import pandas as pd

from autoscalingsim.utils.requirements import ResourceRequirements

class Request:

    """
    Combines static and dynamic information of an individual request/response.
    Since simulator aims at capturing the tail latency bahavior, each request
    matters. The processing times are dynamically updated when the request
    passes through the services. A request becomes a response once its upstream
    flag is dropped.
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
        self._upstream = True
        self.replies_expected = 1

    def set_downstream(self):

        self._upstream = False

    def set_upstream(self):

        self._upstream = True

    @property
    def upstream(self):

        return self._upstream

    @property
    def downstream(self):

        return not self._upstream
