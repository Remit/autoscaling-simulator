import pandas as pd

from ..load.request import Request

class NodeGroupLink:

    """
    Represents transfer of requests over the network.
    """

    def __init__(self,
                 request_processing_infos : dict,
                 latency : pd.Timedelta,
                 network_bandwidth_MBps : int):

        # Static state
        self.latency = latency
        self.bandwidth_MBps = network_bandwidth_MBps
        self.request_processing_infos = request_processing_infos

        # Dynamic state
        self.requests_in_transfer = []
        self.used_bandwidth_MBps = 0

    def step(self,
             simulation_step : pd.Timedelta):

        """ Processing requests to bring them from the link into the buffer """

        requests_for_buffer = []
        for req in self.requests_in_transfer:
            req.cumulative_time += simulation_step
            req.waiting_on_link_left -= simulation_step
            req.network_time += simulation_step

            if req.waiting_on_link_left <= pd.Timedelta(0):
                if req.cumulative_time < self.request_processing_infos[req.request_type].timeout:
                    requests_for_buffer.append(req)
                    self.used_bandwidth_MBps -= self._req_occupied_MBps(req)

                self.requests_in_transfer.remove(req)

        return requests_for_buffer

    def put(self,
            req : Request):

        req_size_b_MBps = self._req_occupied_MBps(req)

        if self.bandwidth_MBps - self.used_bandwidth_MBps >= req_size_b_MBps:

            self.used_bandwidth_MBps += req_size_b_MBps
            req.waiting_on_link_left = self.latency
            self.requests_in_transfer.append(req)
        #else:
        #    del req

    def _req_occupied_MBps(self,
                           req : Request):

        req_size_b = 0
        if not req.upstream:
            req_size_b = self.request_processing_infos[req.request_type].request_size_b
        else:
            req_size_b = self.request_processing_infos[req.request_type].response_size_b
        req_size_b_mb = req_size_b / (1024 * 1024)
        req_size_b_MBps = req_size_b_mb * self.latency.seconds # taking channel for that long
        return req_size_b_MBps
