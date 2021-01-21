import collections
import pandas as pd
import numpy as np

from autoscalingsim.utils.error_check import ErrorChecker

class ServiceScalingInfo:

    DEFAULT_PROVIDER_NAME = 'default'

    def __init__(self, service_name : str, service_scaling_info_raw : dict, scaled_aspect_name : str):

        self.scaled_aspect_name = scaled_aspect_name
        self._booting_duration = collections.defaultdict(lambda: {'mean': 0, 'std': 0, 'unit': 'ms'})
        self._termination_duration = collections.defaultdict(lambda: {'mean': 0, 'std': 0, 'unit': 'ms'})
        self._booting_duration[self.__class__.DEFAULT_PROVIDER_NAME]
        self._termination_duration[self.__class__.DEFAULT_PROVIDER_NAME]

        booting_duration_raw = ErrorChecker.key_check_and_load('booting_duration', service_scaling_info_raw, 'service name', service_name)
        if 'value' in booting_duration_raw:
            self._booting_duration[self.__class__.DEFAULT_PROVIDER_NAME]['mean'] = ErrorChecker.key_check_and_load('value', booting_duration_raw, 'service name', service_name)
            self._booting_duration[self.__class__.DEFAULT_PROVIDER_NAME]['unit'] = ErrorChecker.key_check_and_load('unit', booting_duration_raw, 'service name', service_name)
        elif 'mean' in booting_duration_raw:
            self._update_distribution_parameters(self._booting_duration, self.__class__.DEFAULT_PROVIDER_NAME, provider_specific_config)
        else:
            for provider_name, provider_specific_config in booting_duration_raw.items():
                self._update_distribution_parameters(self._booting_duration, provider_name, provider_specific_config)

        termination_duration_raw = ErrorChecker.key_check_and_load('termination_duration', service_scaling_info_raw, 'service name', service_name)
        if 'value' in termination_duration_raw:
            self._termination_duration[self.__class__.DEFAULT_PROVIDER_NAME]['mean'] = ErrorChecker.key_check_and_load('value', booting_duration_raw, 'service name', service_name)
            self._termination_duration[self.__class__.DEFAULT_PROVIDER_NAME]['unit'] = ErrorChecker.key_check_and_load('unit', booting_duration_raw, 'service name', service_name)
        elif 'mean' in booting_duration_raw:
            self._update_distribution_parameters(self._termination_duration, self.__class__.DEFAULT_PROVIDER_NAME, provider_specific_config)
        else:
            for provider_name, provider_specific_config in termination_duration_raw.items():
                self._update_distribution_parameters(self._termination_duration, provider_name, provider_specific_config)

    @property
    def booting_duration(self):

        return self._sample_duration(self._booting_duration, self.__class__.DEFAULT_PROVIDER_NAME)

    @property
    def termination_duration(self):

        return self._sample_duration(self._termination_duration, self.__class__.DEFAULT_PROVIDER_NAME)

    def get_booting_duration_for_provider(self, provider : str):

        return self._sample_duration(self._booting_duration, provider)

    def get_termination_duration_for_provider(self, provider : str):

        return self._sample_duration(self._termination_duration, provider)

    def _update_distribution_parameters(self, parameters_storage : dict, provider_name : str, provider_specific_config : dict):

        parameters_storage[provider_name]['mean'] = ErrorChecker.key_check_and_load('mean', provider_specific_config, default = None)
        if parameters_storage[provider_name]['mean'] is None:
            parameters_storage[provider_name]['mean'] = ErrorChecker.key_check_and_load('value', provider_specific_config)
        parameters_storage[provider_name]['unit'] = ErrorChecker.key_check_and_load('unit', provider_specific_config)
        if 'std' in provider_specific_config:
            parameters_storage[provider_name]['std'] = ErrorChecker.key_check_and_load('std', provider_specific_config)

    def _sample_duration(self, parameters_storage : dict, provider : str):

        duration_value = max(np.random.normal(parameters_storage[provider]['mean'], parameters_storage[provider]['std']), 0)
        return pd.Timedelta(duration_value, unit = parameters_storage[provider]['unit'])
