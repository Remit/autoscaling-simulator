import collections
import operator
import pandas as pd
import numpy as np

from .error_check import ErrorChecker
from .requirements import ResourceRequirements, CountableResourceRequirement
from .metric.metric_categories.size import Size

class RequestProcessingInfo:

    """
    Wraps static information on how request is processed in the application,
    e.g. in which order it passes through services, which amount of time spends
    there being processed, and which amount of resources it consumes at each
    simulation step.
    """

    @classmethod
    def build_from_config(cls, request_info : dict):

        request_type = ErrorChecker.key_check_and_load('request_type', request_info, 'request')
        entry_service = ErrorChecker.key_check_and_load('entry_service', request_info, 'request_type', request_type)

        processing_times = {}
        processing_times_raw = ErrorChecker.key_check_and_load('processing_times', request_info, 'request_type', request_type)
        processing_times_unit = ErrorChecker.key_check_and_load('unit', processing_times_raw, 'request_type', request_type)
        processing_times_vals = ErrorChecker.key_check_and_load('values', processing_times_raw, 'request_type', request_type)
        for processing_time_service_entry in processing_times_vals:

            service_name = ErrorChecker.key_check_and_load('service', processing_time_service_entry, 'request_type', request_type)
            upstream_time = ErrorChecker.key_check_and_load('upstream', processing_time_service_entry, 'request_type', request_type)
            downstream_time = ErrorChecker.key_check_and_load('downstream', processing_time_service_entry, 'request_type', request_type)

            processing_times[service_name] = { 'upstream' : cls._update_distribution_parameters(upstream_time, processing_times_unit), \
                                               'downstream' : cls._update_distribution_parameters(downstream_time, processing_times_unit) }

        timeout_raw = ErrorChecker.key_check_and_load('timeout', request_info, 'request_type', request_type)
        timeout_val = ErrorChecker.key_check_and_load('value', timeout_raw, 'request_type', request_type)
        timeout_unit = ErrorChecker.key_check_and_load('unit', timeout_raw, 'request_type', request_type)
        timeout = pd.Timedelta(timeout_val, unit = timeout_unit)
        ErrorChecker.value_check('timeout', timeout, operator.ge, pd.Timedelta(0, unit = timeout_unit), [f'request_type {request_type}'])

        request_size = CountableResourceRequirement.memory_from_dict(ErrorChecker.key_check_and_load('request_size', request_info, 'request_type', request_type))
        response_size = CountableResourceRequirement.memory_from_dict(ErrorChecker.key_check_and_load('response_size', request_info, 'request_type', request_type))

        request_operation_type = ErrorChecker.key_check_and_load('operation_type', request_info, 'request_type', request_type)

        request_processing_requirements = ErrorChecker.key_check_and_load('processing_requirements', request_info, 'request_type', request_type)

        return cls(request_type, entry_service, processing_times, timeout, request_size, response_size, request_operation_type, request_processing_requirements)

    @classmethod
    def _update_distribution_parameters(cls, config : dict, processing_times_unit : str):

        distribution_parameters = {'mean': 0, 'std': 0, 'unit': processing_times_unit}

        if isinstance(config, collections.Iterable):
            distribution_parameters['mean'] = ErrorChecker.key_check_and_load('mean', config, default = None)
            if distribution_parameters['mean'] is None:
                distribution_parameters['mean'] = ErrorChecker.key_check_and_load('value', config)
            distribution_parameters['unit'] = ErrorChecker.key_check_and_load('unit', config, default = processing_times_unit)
            if 'std' in config:
                distribution_parameters['std'] = ErrorChecker.key_check_and_load('std', config)

        else:
            distribution_parameters['mean'] = config

        return distribution_parameters

    def __init__(self,
                 request_type : str,
                 entry_service : str,
                 processing_times : dict,
                 timeout : pd.Timedelta,
                 request_size : CountableResourceRequirement,
                 response_size : CountableResourceRequirement,
                 request_operation_type : str,
                 request_processing_requirements : dict):

        self.request_type = request_type
        self.entry_service = entry_service
        self.processing_times = processing_times
        self.timeout = timeout
        self.request_size = request_size
        self.response_size = response_size
        self.request_operation_type = request_operation_type
        self.resource_requirements = ResourceRequirements.from_dict(request_processing_requirements)

    def get_upstream_processing_time(self, service_name : str) -> pd.Timedelta:

        return self._sample_processing_time(service_name, 'upstream')

    def get_downstream_processing_time(self, service_name : str) -> pd.Timedelta:

        return self._sample_processing_time(service_name, 'downstream')

    def _sample_processing_time(self, service_name : str, direction : str) -> pd.Timedelta:

        processing_time_value = max(np.random.normal(self.processing_times[service_name][direction]['mean'], self.processing_times[service_name][direction]['std']), 0)
        return pd.Timedelta(processing_time_value, unit = self.processing_times[service_name][direction]['unit'])
