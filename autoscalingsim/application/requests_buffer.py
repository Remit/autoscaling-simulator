import pandas as pd
from collections import deque

from ..load.request import Request
from ..infrastructure_platform.link import NodeGroupLink

class RequestsBuffer:

    """
    Represents buffers where the requests wait to be served.
    """

    def __init__(self,
                 capacity_by_request_type : dict,
                 policy = "FIFO"):

        # Static state
        self.capacity_by_request_type = capacity_by_request_type
        self.policy = policy

        # Dynamic state
        self.link = None
        self.requests = deque([])
        self.reqs_cnt = {}
        for request_type in capacity_by_request_type.keys():
            self.reqs_cnt[request_type] = 0

    def step(self,
             simulation_step : pd.Timedelta):

        if not self.link is None:
            ready_reqs = self.link.step(simulation_step)

            for req in ready_reqs:
                # If there is not spare capacity, then the request is lost
                if self.capacity_by_request_type[req.request_type] > self.reqs_cnt[req.request_type]:
                    self.reqs_cnt[req.request_type] += 1
                    self.requests.append(req)

    def put(self,
            req : Request):

        # Request is lost if link was not yet established
        if not self.link is None:
            self.link.put(req)

    def set_link(self,
                 link : NodeGroupLink):
        self.link = link

    def attempt_pop(self):

        """ Provides a copy of the oldest request in the queue """

        if self.size() > 0:
            return self.requests[-1]
        else:
            return None

    def attempt_fan_in(self):

        """
        Attempts to find all the requests that are required to process the oldest
        request in the queue. If all prerequisites are fulfilled, the copy of the oldest request
        is returned. Otherwise, this method returns None.
        """

        if self.size() > 0:
            req = self.requests[-1]
            # Processing fan-in case
            if req.replies_expected > 1:
                req_id_ref = req.request_id
                reqs_present = 1
                for req_lookup in self.requests[:-1]:
                    if req_lookup.request_id == req_id_ref:
                        reqs_present += 1

                if reqs_present == req.replies_expected:
                    return req

            else:
                return req
        else:
            return None

    def shift(self):

        """
        Shifts requests queue to give other requests a chance to get processed.
        """

        req = self.pop()
        self.append_left(req)

    def fan_in(self,
               req : Request):

        """
        Finalizes fan-in action for the given request by removing the
        associated responses from the requests queue.
        """

        req = self.pop()
        self.remove_by_id(req.request_id)
        req.replies_expected = 1

        return req

    def append_left(self, req):

        self.requests.appendleft(req)

    def pop(self):

        req = None
        if len(self.requests) > 0:
            req = self.requests.pop()
            self.reqs_cnt[req.request_type] -= 1

        return req

    def pop_left(self):

        req = None
        if len(self.requests) > 0:
            req = self.requests.popLeft()
            self.reqs_cnt[req.request_type] -= 1

        return req

    def size(self):

        return len(self.requests)

    def add_cumulative_time(self,
                            delta : pd.Timedelta,
                            service_name : str):

        for req in self.requests:
            req.cumulative_time += delta
            # Below is for the monitoring purposes - to estimate,
            # which service did the request spend the longest time waiting at
            if not service_name in req.buffer_time:
                req.buffer_time[service_name] = delta
            else:
                req.buffer_time[service_name] += delta

    def remove_by_id(self,
                     request_id : int):

        for req in reversed(self.requests):
            if req.request_id == request_id:
                self.requests.remove(req)
