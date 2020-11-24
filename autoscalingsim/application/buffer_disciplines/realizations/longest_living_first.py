import bisect
import pandas as pd

from autoscalingsim.application.buffer_disciplines.discipline import QueuingDiscipline
from autoscalingsim.load.request import Request

@QueuingDiscipline.register('OF')
class OldestFirstQueue(QueuingDiscipline):

    """
    Implements a queuing discipline that organizes the requests by their
    cumulative time existing in the application. Priority to leave the queue
    according to this discipline is given to the request that has the highest
    cumulative time spent in the application.
    """

    def __init__(self):

        super().__init__()
        self.sorted_times_ascending = []

    def insert(self, req : Request):

        bisect.insort(self.sorted_times_ascending, req.cumulative_time)
        self.requests.insert(self.sorted_times_ascending.index(req.cumulative_time), req)

    def attempt_take(self):

        """
        Provides a copy of the request that spent the most time in
        the application overall, inc. in the service buffers and on the network.
        """

        if len(self.requests) > 0:
            return self.requests[-1]
        else:
            return None

    def take(self):

        req = None
        if len(self.requests) > 0:
            req = self.requests.pop()
            self.sorted_times_ascending.remove(req.cumulative_time)

        return req

    def shuffle(self):

        """ Not implemented """

        pass

    def add_cumulative_time(self, delta : pd.Timedelta, service_name : str):

        for req in self.requests:
            req.cumulative_time += delta
            # Below is for the monitoring purposes - to estimate,
            # which service did the request spend the longest time waiting at
            if not service_name in req.buffer_time:
                req.buffer_time[service_name] = delta
            else:
                req.buffer_time[service_name] += delta

        self.sorted_times_ascending = [time_sorted + delta for time_sorted in self.sorted_times_ascending]
