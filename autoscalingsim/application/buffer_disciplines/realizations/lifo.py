import pandas as pd

from ..discipline import QueuingDiscipline

from ....load.request import Request

@QueuingDiscipline.register('LIFO')
class LIFOQueue(QueuingDiscipline):

    """
    Implements Last In-First Out (LIFO) queuing discipline.
    """

    def insert(self, req : Request):

        self.requests.append(req)

    def attempt_take(self):

        """ Provides a copy of the last request added """

        if len(self.requests) > 0:
            return self.requests[-1]
        else:
            return None

    def take(self):

        req = None
        if len(self.requests) > 0:
            req = self.requests.pop()

        return req

    def shuffle(self):

        req = self.take()
        self.requests.appendleft(req)

    def add_cumulative_time(self, delta : pd.Timedelta, service_name : str):

        for req in self.requests:
            req.cumulative_time += delta
            # Below is for the monitoring purposes - to estimate,
            # which service did the request spend the longest time waiting at
            if not service_name in req.buffer_time:
                req.buffer_time[service_name] = delta
            else:
                req.buffer_time[service_name] += delta
