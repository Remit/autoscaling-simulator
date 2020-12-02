import collections
import pandas as pd

from autoscalingsim.load.request import Request

class RequestsProcessor:

    def __init__(self):

        self.in_processing_simultaneous = collections.defaultdict(list)
        self.out = collections.defaultdict(list)
        self.stat = collections.defaultdict(lambda: collections.defaultdict(int))

    def _processing_goes_on_and_there_is_enough_time(self, advancing : bool, time_budget : pd.Timedelta):

        return advancing and any([len(processing_list) for processing_list in self.in_processing_simultaneous.values()]) and time_budget > pd.Timedelta(0, unit ='ms')

    def step(self, time_budget : pd.Timedelta):

        advancing = True
        while self._processing_goes_on_and_there_is_enough_time(advancing, time_budget):
            advancing = False

            # Find a minimal leftover duration, subtract it, and propagate the request
            min_leftover_time = min([req.processing_time_left for processing_list in self.in_processing_simultaneous.values() for req in processing_list])
            min_time_to_subtract = min(min_leftover_time, time_budget)
            if min_time_to_subtract > pd.Timedelta(0, unit ='ms'): advancing = True

            for service_name, processing_list in self.in_processing_simultaneous.items():
                new_in_processing_simultaneous = []
                for req in processing_list:
                    new_time_left = req.processing_time_left - min_time_to_subtract

                    req.cumulative_time += min_time_to_subtract
                    if new_time_left > pd.Timedelta(0, unit = 'ms'):
                        req.processing_time_left = new_time_left
                        new_in_processing_simultaneous.append(req)

                    else:
                        advancing = True # advancing on the processing of a request being done
                        req.processing_time_left = pd.Timedelta(0, unit = 'ms')
                        self.stat[service_name][req.request_type] -= 1
                        self.out[service_name].append(req)

                self.in_processing_simultaneous[service_name] = new_in_processing_simultaneous

            time_budget -= min_time_to_subtract

        return time_budget

    def start_processing(self, req : Request):

        self.stat[req.processing_service][req.request_type] += 1
        self.in_processing_simultaneous[req.processing_service].append(req)

    def processed_for_service(self, service_name : str):

        processed_for_service = []
        if service_name in self.out:
            processed_for_service.extend(self.out[service_name])
            self.out[service_name] = []

        return processed_for_service

    def in_processing_stat_for_service(self, service_name : str):

        return self.stat.get(service_name, {})

    @property
    def services_ever_scheduled(self):

        return self.stat.keys()
