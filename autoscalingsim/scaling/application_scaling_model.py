import pandas as pd
from ..utils.error_check import ErrorChecker

class ServiceScalingInfo:
    """
    """
    def __init__(self,
                 boot_up_delta,
                 termination_ms = pd.Timedelta(0, unit = 'ms')):

        self.boot_up_ms = boot_up_delta
        self.termination_ms = termination_ms

class ServiceScalingInfoIterator:

    def __init__(self,
                 application_scaling_model):
        self._index = 0
        self._application_scaling_model = application_scaling_model

    def __next__(self):

        if self._index < len(self._application_scaling_model.service_scaling_infos):
            ssi = self._application_scaling_model.service_scaling_infos[self._application_scaling_model.service_scaling_infos.keys()[self._index]]
            self._index += 1
            return ssi

        raise StopIteration

class ApplicationScalingModel:
    """
    """
    def __init__(self,
                 decision_making_time_ms = 0,
                 service_scaling_infos_raw = []):

        self.decision_making_time_ms = decision_making_time_ms
        self.service_scaling_infos = {}

        for service_scaling_info_raw in service_scaling_infos_raw:

            boot_up_ms = ErrorChecker.key_check_and_load('boot_up_ms', service_scaling_info_raw, self.__class__.__name__)
            termination_ms = ErrorChecker.key_check_and_load('termination_ms', service_scaling_info_raw, self.__class__.__name__)
            ssi = ServiceScalingInfo(pd.Timedelta(boot_up_ms, unit = 'ms'),
                                     pd.Timedelta(termination_ms, unit = 'ms'))

            service_name = ErrorChecker.key_check_and_load('name', service_scaling_info_raw, self.__class__.__name__)

            self.service_scaling_infos[service_name] = ssi

    def __iter__(self):
        return ServiceScalingInfoIterator(self)

    def get_service_scaling_params(self,
                                   service_name):
        ssi = None
        if service_name in self.service_scaling_infos:
            ssi = self.service_scaling_infos[service_name]

        return ssi

    def get_entities_with_expired_scaling_period(self,
                                                 interval : pd.Timedelta):

        entities_booting_period_expired = []
        entities_termination_period_expired = []
        for entity_name, ssi in self.service_scaling_infos.items():
            if ssi.boot_up_ms <= interval:
                entities_booting_period_expired.append(entity_name)
            if ssi.termination_ms <= interval:
                entities_termination_period_expired.append(entity_name)

        return (entities_booting_period_expired, entities_termination_period_expired)
