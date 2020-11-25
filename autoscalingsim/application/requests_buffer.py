import pandas as pd

from .buffer_disciplines.discipline import QueuingDiscipline
from .buffer_utilization import BufferUtilization

from autoscalingsim.load.request import Request
from autoscalingsim.infrastructure_platform.link import NodeGroupLink

class RequestsBuffer:

    """ Represents buffers where the requests wait to be served """

    def __init__(self, capacity_by_request_type_base : dict, queuing_discipline : str = 'FIFO'):

        self.capacity_by_request_type_base = capacity_by_request_type_base
        self.capacity_by_request_type = self.capacity_by_request_type_base.copy() # updatable
        self.discipline = QueuingDiscipline.get(queuing_discipline)()
        self.reqs_cnt = { request_type : 0 for request_type in self.capacity_by_request_type_base }
        self.links = []

        self.links_index = {}
        self.last_used_link_id = 0
        self.utilization = BufferUtilization()

    def get_metric_value(self, metric_name : str, interval : pd.Timedelta):

        return self.utilization.get(metric_name, interval)

    def step(self, simulation_step : pd.Timedelta):

        for link in self.links:
            ready_reqs = link.step(simulation_step)

            for req in ready_reqs:
                if req.request_type in self.capacity_by_request_type:
                    # If there is not spare capacity, then the request is lost.
                    # An unbounded addition of requests to the buffer is allowed
                    # either if the capacity is set to zero or if it is absent.
                    if self.capacity_by_request_type[req.request_type] == 0 or self.capacity_by_request_type[req.request_type] > self.reqs_cnt[req.request_type]:
                        self.reqs_cnt[req.request_type] += 1
                        self.discipline.insert(req)
                else:
                    self.reqs_cnt[req.request_type] += 1
                    self.discipline.insert(req)

    def update_utilization(self, cur_timestamp : pd.Timestamp,  averaging_interval : pd.Timedelta):

        self.utilization.update_waiting_time(cur_timestamp,
                                             self.discipline.get_average_waiting_time(),
                                             averaging_interval)

        self.utilization.update_waiting_requests_count(cur_timestamp, sum(self.reqs_cnt.values()),
                                                       averaging_interval)

    def update_capacity(self, service_instances_count : int):

        if not isinstance(service_instances_count, int):
            raise ValueError(f'Cannot change buffer capacity using non-int: {service_instances_count}')

        for req_type, init_capacity in self.capacity_by_request_type_base.items():
            self.capacity_by_request_type[req_type] = init_capacity * service_instances_count

    def put(self, req : Request, simulation_step : pd.Timedelta):

        # Request is lost if link was not yet established
        if len(self.links) > 0:
            self.links[self.last_used_link_id].put(req, simulation_step)
            self.last_used_link_id = self.last_used_link_id + 1 if self.last_used_link_id < len(self.links) - 1 else 0

    def add_link(self, node_group_id : int, link : NodeGroupLink):

        self.links.append(link)
        self.links_index[node_group_id] = len(self.links) - 1

    def detach_link(self, node_group_id : int):

        if node_group_id in self.links:
            ordinal_index = self.links_index[node_group_id]
            self.links.remove(self.links[ordinal_index])
            del self.links_index[node_group_id]
            self.last_used_link_id = 0

    def attempt_take(self):

        return self.discipline.attempt_take()

    def attempt_fan_in(self):

        return self.discipline.attempt_fan_in()

    def take(self):

        req = self.discipline.take()
        if not req is None:
            if req.request_type in self.reqs_cnt:
                self.reqs_cnt[req.request_type] -= 1

        return req

    def fan_in(self):

        """
        Finalizes fan-in action for the given request by removing the
        associated responses from the requests queue.
        """

        req = self.discipline.take()
        if not req is None:
            self.discipline.remove_by_id(req.request_id)
            req.replies_expected = 1

        return req

    def shuffle(self):

        """
        Shuffles requests to give more opportunities to every awaiting request
        to be processed.
        """

        self.discipline.shuffle()

    def size(self):

        return self.discipline.size()

    def add_cumulative_time(self, delta : pd.Timedelta, service_name : str):

        self.discipline.add_cumulative_time(delta, service_name)
