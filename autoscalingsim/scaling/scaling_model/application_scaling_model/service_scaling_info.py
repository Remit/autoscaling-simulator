import pandas as pd

from autoscalingsim.utils.error_check import ErrorChecker

class ServiceScalingInfo:

    def __init__(self, service_name : str, service_scaling_info_raw : dict, scaled_aspect_name : str):

        self.scaled_aspect_name = scaled_aspect_name

        booting_duration_raw = ErrorChecker.key_check_and_load('booting_duration', service_scaling_info_raw, 'service name', service_name)
        booting_duration_value = ErrorChecker.key_check_and_load('value', booting_duration_raw, 'service name', service_name)
        booting_duration_unit = ErrorChecker.key_check_and_load('unit', booting_duration_raw, 'service name', service_name)
        self._booting_duration = pd.Timedelta(booting_duration_value, unit = booting_duration_unit)

        termination_duration_raw = ErrorChecker.key_check_and_load('termination_duration', service_scaling_info_raw, 'service name', service_name)
        termination_duration_value = ErrorChecker.key_check_and_load('value', termination_duration_raw, 'service name', service_name)
        termination_duration_unit = ErrorChecker.key_check_and_load('unit', termination_duration_raw, 'service name', service_name)
        self._termination_duration = pd.Timedelta(termination_duration_value, unit = termination_duration_unit)

    @property
    def booting_duration(self):

        return self._booting_duration

    @property
    def termination_duration(self):

        return self._termination_duration
