import pandas as pd

from abc import ABC, abstractmethod
from collections import deque

from autoscalingsim.load.request import Request

class QueuingDiscipline(ABC):

    """
    An interface for queuing disciplines set for buffers.
    An implementation should subclass from this class, implement its abstract
    methods, and register with this class. All the implementations are to be placed
    into the *realizations* folder. Upon adding a new discipline, __init__.py in the
    realizations folder has to be accordingly adjusted.
    """

    _Registry = {}

    @classmethod
    def register(cls, name : str):

        def decorator(queuing_discipline_class):
            cls._Registry[name] = queuing_discipline_class
            return queuing_discipline_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent queuing discipline {name}')

        return cls._Registry[name]

    @abstractmethod
    def insert(self, req : Request):

        """
        Inserts the provided request into the queue
        according to the implementing discipline
        """

        pass

    @abstractmethod
    def attempt_take(self):

        """
        Provides a copy of the 'first in line' request
        (according to the discipline that realizes this method)
        """

        pass

    @abstractmethod
    def take(self):

        """
        Extracts the 'first in line' request
        (according to the discipline that realizes this method)
        """

        pass

    @abstractmethod
    def shuffle(self):

        """
        Shuffles the queue according to the specific discipline
        """

        pass

    @abstractmethod
    def add_cumulative_time(self, delta : pd.Timedelta, service_name : str):

        pass

    def __init__(self):

        self.requests = deque([])

    def attempt_fan_in(self):

        """
        Attempts to find all the requests that are required to process the oldest
        request in the queue. If all prerequisites are fulfilled, the copy of the oldest request
        is returned. Otherwise, this method returns None.
        """

        req = self.attempt_take()
        if not req is None:
            # Processing fan-in case
            if req.replies_expected > 1:
                req_id_ref = req.request_id
                reqs_present = 0
                for req_lookup in self.requests:
                    if req_lookup.request_id == req_id_ref:
                        reqs_present += 1

                if reqs_present == req.replies_expected:
                    return req
                else:
                    return None # Not all parts of the request arrived yet

        return req

    def remove_by_id(self, request_id : int):

        """
        Removes all the requests forming the multi-part request with the given
        request_id.
        """

        for req in reversed(self.requests):
            if req.request_id == request_id:
                self.requests.remove(req)

    def size(self):

        return len(self.requests)

    def get_average_waiting_time(self):

        """
        Returns an average waiting time of all the requests currently queued.
        """

        if len(self.requests) == 0:
            return 0
        else:
            return sum([req.buffer_time.get(req.processing_service, pd.Timedelta(0, unit = 'ms')) for req in self.requests], pd.Timedelta(0, unit = 'ms')) / len(self.requests)

from .realizations import *
