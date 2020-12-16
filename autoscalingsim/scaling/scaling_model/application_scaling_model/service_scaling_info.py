import pandas as pd

from autoscalingsim.utils.error_check import ErrorChecker

class ServiceScalingInfo:

    DEFAULT_PROVIDER_NAME = 'default'

    def __init__(self, service_name : str, service_scaling_info_raw : dict, scaled_aspect_name : str):

        self.scaled_aspect_name = scaled_aspect_name
        self._booting_duration = dict()
        self._termination_duration = dict()

        self._booting_duration[self.__class__.DEFAULT_PROVIDER_NAME] = pd.Timedelta(0, unit ='ms')
        self._termination_duration[self.__class__.DEFAULT_PROVIDER_NAME] = pd.Timedelta(0, unit ='ms')

        booting_duration_raw = ErrorChecker.key_check_and_load('booting_duration', service_scaling_info_raw, 'service name', service_name)
        if 'value' in booting_duration_raw:
            self._booting_duration[self.__class__.DEFAULT_PROVIDER_NAME] = self._construct_duration(booting_duration_raw, service_name)
        else:
            for provider_name, provider_specific_config in booting_duration_raw.items():
                self._booting_duration[provider_name] = self._construct_duration(provider_specific_config, service_name)

        termination_duration_raw = ErrorChecker.key_check_and_load('termination_duration', service_scaling_info_raw, 'service name', service_name)
        if 'value' in termination_duration_raw:
            self._termination_duration[self.__class__.DEFAULT_PROVIDER_NAME] = self._construct_duration(termination_duration_raw, service_name)
        else:
            for provider_name, provider_specific_config in termination_duration_raw.items():
                self._termination_duration[provider_name] = self._construct_duration(provider_specific_config, service_name)

    @property
    def booting_duration(self):

        return self._booting_duration[self.__class__.DEFAULT_PROVIDER_NAME]

    @property
    def termination_duration(self):

        return self._termination_duration[self.__class__.DEFAULT_PROVIDER_NAME]

    def get_booting_duration_for_provider(self, provider : str):

        return self._booting_duration.get(provider, self._booting_duration[self.__class__.DEFAULT_PROVIDER_NAME])

    def get_termination_duration_for_provider(self, provider : str):

        return self._termination_duration.get(provider, self._termination_duration[self.__class__.DEFAULT_PROVIDER_NAME])

    def _construct_duration(self, duration_raw : dict, service_name : str):

        duration_value = ErrorChecker.key_check_and_load('value', duration_raw, 'service name', service_name)
        duration_unit = ErrorChecker.key_check_and_load('unit', duration_raw, 'service name', service_name)
        return pd.Timedelta(duration_value, unit = duration_unit)
