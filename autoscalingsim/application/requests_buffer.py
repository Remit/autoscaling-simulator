import pandas as pd

from .buffer_disciplines.discipline import QueuingDiscipline
from ..load.request import Request
from ..infrastructure_platform.link import NodeGroupLink

class RequestsBuffer:

    """
    Represents buffers where the requests wait to be served.
    """

    waiting_time_metric_name = 'waiting_time'
    waiting_requests_count_metric_name = 'waiting_requests_count'

    def __init__(self,
                 capacity_by_request_type : dict,
                 queuing_discipline : str = 'FIFO'):

        # Static state
        self.capacity_by_request_type = capacity_by_request_type

        # Dynamic state
        self.link = None
        self.discipline = QueuingDiscipline.get(queuing_discipline)()
        self.reqs_cnt = {}
        for request_type in capacity_by_request_type.keys():
            self.reqs_cnt[request_type] = 0

    def get_metric_value(self, metric_name : str):

        if metric_name == self.__class__.waiting_time_metric_name:
            return self.discipline.get_average_waiting_time()
        elif metric_name == self.__class__.waiting_requests_count_metric_name:
            return sum(self.reqs_cnt.values())
        else:
            raise ValueError(f'Unknown metric {metric_name} to get from {self.__class__.__name__}')

    def step(self,
             simulation_step : pd.Timedelta):

        if not self.link is None:
            ready_reqs = self.link.step(simulation_step)

            for req in ready_reqs:
                # If there is not spare capacity, then the request is lost
                if self.capacity_by_request_type[req.request_type] > self.reqs_cnt[req.request_type]:
                    self.reqs_cnt[req.request_type] += 1
                    self.discipline.insert(req)

    def put(self,
            req : Request):

        # Request is lost if link was not yet established
        if not self.link is None:
            self.link.put(req)

    def set_link(self,
                 link : NodeGroupLink):
        self.link = link

    def attempt_take(self):

        return self.discipline.attempt_take()

    def attempt_fan_in(self):

        return self.discipline.attempt_fan_in()

    def take(self):

        req = self.discipline.take()
        if not req is None:
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
        Shifts requests queue to give other requests a chance to get processed.
        """

        self.discipline.shuffle()

    def size(self):

        return self.discipline.size()

    def add_cumulative_time(self,
                            delta : pd.Timedelta,
                            service_name : str):

        self.discipline.add_cumulative_time(delta, service_name)
