import pandas as pd

from ..load.request import Request

class NodeGroupLink:

    """
    Represents transfer of requests over the network.

    Attributes:

        latency (pd.Timedelta): determines how much time does the request
            spend being transferred over this link.
            TODO: make non-deterministic?

        single_link_network_bandwidth_MBps (int): determines the bandwidth of
            a single link, i.e. a link bandwidth provided with a single deployed
            node. Used in scaling the overall bandwidth of the node group link
            by being multiplied by the new count of nodes in the node group.
            Does not change.

        request_processing_infos (dict from request type -> RequestProcessingInfo):
            keeps processing/transferring constants for every request type.
            Used to determine whether the new request fits in the remaining throughput
            and whether it should be dismissed since the timeout was already hit.

        bandwidth_MBps (int): current link bandwidth as determined for the whole
            associated node group.

        requests_in_transfer (list of Request): requests that are currently being
            transferred over this link.

        used_bandwidth_MBps (int): bandwidth taken by the requests that are
            currently in transfer over this link.

    """

    def __init__(self,
                 latency : pd.Timedelta,
                 count_of_nodes_in_group : int,
                 single_link_network_bandwidth_MBps : int):

        self.latency = latency
        self.single_link_network_bandwidth_MBps = single_link_network_bandwidth_MBps
        self.request_processing_infos = None

        self.bandwidth_MBps = self.single_link_network_bandwidth_MBps * count_of_nodes_in_group
        self.requests_in_transfer = []
        self.used_bandwidth_MBps = 0

    def step(self, simulation_step : pd.Timedelta):

        """ Processing requests to bring them from the link into the buffer """

        requests_for_buffer = []
        for req in self.requests_in_transfer:
            req.cumulative_time += simulation_step
            req.waiting_on_link_left -= simulation_step
            req.network_time += simulation_step

            if req.waiting_on_link_left <= pd.Timedelta(0):
                if req.cumulative_time < self.request_processing_infos[req.request_type].timeout:
                    requests_for_buffer.append(req)
                    self.used_bandwidth_MBps -= self._req_occupied_MBps(req, simulation_step)

                self.requests_in_transfer.remove(req)

        return requests_for_buffer

    def put(self, req : Request, simulation_step : pd.Timedelta):

        req_size_b_MBps = self._req_occupied_MBps(req, simulation_step)

        if self.bandwidth_MBps - self.used_bandwidth_MBps >= req_size_b_MBps:

            self.used_bandwidth_MBps += req_size_b_MBps
            req.waiting_on_link_left = self.latency
            self.requests_in_transfer.append(req)

    def set_request_processing_infos(self, request_processing_infos : dict):

        self.request_processing_infos = request_processing_infos

    def update_bandwidth(self, new_nodes_count : int):

        self.bandwidth_MBps = self.single_link_network_bandwidth_MBps * new_nodes_count

    def _req_occupied_MBps(self, req : Request, simulation_step : pd.Timedelta):

        req_size_b = self.request_processing_infos[req.request_type].request_size_b if req.upstream else self.request_processing_infos[req.request_type].response_size_b
        req_size_b_mb = req_size_b / (1024 * 1024)
        req_size_b_MBps = req_size_b_mb * (self.latency // simulation_step) # taking channel for that long

        return req_size_b_MBps
