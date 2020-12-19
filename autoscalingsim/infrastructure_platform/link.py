import pandas as pd

from .node_information.node import NodeInfo

from autoscalingsim.load.request import Request
from autoscalingsim.utils.size import Size

class NodeGroupLink:

    """
    Represents transfer of requests over the network.

    Attributes:

        latency (pd.Timedelta): determines how much time does the request
            spend being transferred over this link.
            TODO: make non-deterministic?

        single_link_network_bandwidth (int): determines the bandwidth of
            a single link, i.e. a link bandwidth provided with a single deployed
            node. Used in scaling the overall bandwidth of the node group link
            by being multiplied by the new count of nodes in the node group.
            Does not change.

        request_processing_infos (dict from request type -> RequestProcessingInfo):
            keeps processing/transferring constants for every request type.
            Used to determine whether the new request fits in the remaining throughput
            and whether it should be dismissed since the timeout was already hit.

        bandwidth (int): current link bandwidth as determined for the whole
            associated node group.

        requests_in_transfer (list of Request): requests that are currently being
            transferred over this link.

        used_bandwidth (int): bandwidth taken by the requests that are
            currently in transfer over this link.

    """

    def __init__(self, node_info : NodeInfo, count_of_nodes_in_group : int):

        self.latency = node_info.latency
        self.single_link_network_bandwidth = node_info.network_bandwidth

        self.bandwidth = self.single_link_network_bandwidth * count_of_nodes_in_group
        self.requests_in_transfer = []
        self.used_bandwidth = Size(0)

    def step(self):

        """ Processing requests to bring them from the link into the buffer """

        requests_for_buffer = []
        for req in self.requests_in_transfer:
            req.cumulative_time += req.simulation_step
            req.waiting_on_link_left -= req.simulation_step
            req.network_time += req.simulation_step

            if req.waiting_on_link_left <= pd.Timedelta(0):
                if req.cumulative_time < req.timeout:
                    requests_for_buffer.append(req)
                    self.used_bandwidth -= self._req_occupied(req)

                self.requests_in_transfer.remove(req)

        return requests_for_buffer

    def transfer_requests_to(self, target : 'NodeGroupLink'):

        for req in self.requests_in_transfer:
            if not target.can_accept_request(req):
                break

            target.put(req)

        for req in target.requests_in_transfer:
            if req in self.requests_in_transfer:
                self.requests_in_transfer.remove(req)            

    def can_accept_request(self, req : Request):

        return self.bandwidth - self.used_bandwidth >= self._req_occupied(req)

    def put(self, req : Request):

        req_size = self._req_occupied(req)

        if self.bandwidth - self.used_bandwidth >= req_size:

            self.used_bandwidth += req_size
            req.waiting_on_link_left = self.latency
            self.requests_in_transfer.append(req)

    def update_bandwidth(self, new_nodes_count : int):

        self.bandwidth = self.single_link_network_bandwidth * new_nodes_count

    def _req_occupied(self, req : Request):

        req_size = req.request_size if req.upstream else req.response_size
        return req_size * (self.latency // req.simulation_step) # taking channel for that many simulation steps * by the taken bandwidth
