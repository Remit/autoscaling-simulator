import collections
import pandas as pd
from copy import deepcopy

from autoscalingsim.load.request import Request
from autoscalingsim.utils.requirements import ResourceRequirements, ResourceRequirementsSample

class RequestsProcessor:

    def __init__(self):

        self.in_processing_simultaneous = collections.defaultdict(list)
        self._out = collections.defaultdict(list)
        self._stat = collections.defaultdict(lambda: collections.defaultdict(int))
        self._res_requirements_of_requests = collections.defaultdict(lambda: collections.defaultdict(ResourceRequirements))

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
                        self._stat[service_name][req.request_type] -= 1
                        self._out[service_name].append(req)
                        if self._stat[service_name][req.request_type] == 0:
                            del self._res_requirements_of_requests[service_name][req.request_type]

                self.in_processing_simultaneous[service_name] = new_in_processing_simultaneous

            time_budget -= min_time_to_subtract

        return time_budget

    def start_processing(self, req : Request):

        self._stat[req.processing_service][req.request_type] += 1
        self._res_requirements_of_requests[req.processing_service][req.request_type] = req.resource_requirements
        self.in_processing_simultaneous[req.processing_service].append(req)

    def processed_for_service(self, service_name : str):

        processed_for_service = []
        if service_name in self._out:
            processed_for_service.extend(self._out[service_name])
            self._out[service_name] = []

        return processed_for_service

    def in_processing_stat_for_service(self, service_name : str):

        return self._stat.get(service_name, {})

    def service_instances_fraction_in_use_for_service(self, service_name : str):

        result = 0
        for req_type, req_cnt in self._stat.get(service_name, {None: 0}).items():
            if not req_type is None:
                req_vCPU_usage = self._res_requirements_of_requests.get(service_name, dict()).get(req_type, ResourceRequirementsSample()).vCPU.value
                result += req_cnt * req_vCPU_usage

        return result

    def requests_counts_and_requirements_for_service(self, service_name : str):

        if not service_name in self._stat:
            return zip([], [])
        else:
            return zip(self._stat[service_name].values(), self._res_requirements_of_requests[service_name].values())

    def in_processing_simultaneous_flat(self):

        return InProcessingRequestsIterator([ req for requests_for_service in self.in_processing_simultaneous.items() for req in requests_for_service ])

    def extract_out(self):

        result = self._out
        self._out = None
        return result

    def remove_in_processing_request(self, req : Request):

        if req.processing_service in self.in_processing_simultaneous:
            if req in self.in_processing_simultaneous[req.processing_service]:
                self.in_processing_simultaneous[req.processing_service].remove(req)

    @property
    def out(self):

        return self._out

    @out.setter
    def out(self, other_out):

        if not other_out is None:
            self._out = deepcopy(other_out)

    @property
    def stat(self):

        return self._stat

    @property
    def services_ever_scheduled(self):

        return self._stat.keys()

class InProcessingRequestsIterator:

    def __init__(self, flat_requests_list):

        self._requests = flat_requests_list
        self._cur_index = 0

    def __iter__(self):

        return self

    def __next__(self):

        if self._cur_index < len(self._requests):
            req = self._requests[self._cur_index]
            self._cur_index += 1
            return req

        raise StopIteration
