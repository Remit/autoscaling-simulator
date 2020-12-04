import pandas as pd
from collections import OrderedDict
from .service_scaling_info import ServiceScalingInfo

from autoscalingsim.utils.error_check import ErrorChecker

class ApplicationScalingModel:

    def __init__(self, service_scaling_infos_raw : list, services_scaling_config : dict):

        self.service_scaling_infos = dict()
        default_service_conf = services_scaling_config.get('default', None)
        for service_scaling_info_raw in service_scaling_infos_raw:
            service_name = ErrorChecker.key_check_and_load('name', service_scaling_info_raw)
            service_conf = services_scaling_config.get(service_name, default_service_conf)
            scaled_aspect_name = service_conf.scaled_aspect_name if not service_conf is None else 'count'
            self.service_scaling_infos[service_name] = ServiceScalingInfo(service_name, service_scaling_info_raw, scaled_aspect_name)

    def delay(self, services_group_delta : 'GroupOfServicesDelta'):

        """ Returns multiple delayed services deltas indexed by their delays """

        if not services_group_delta is None:
            services_by_change_enforcement_delay = self._group_services_by_change_enforcement_time(services_group_delta)
            return self._enforce_services_of_delta_by_delay(services_group_delta, services_by_change_enforcement_delay)
            
        else:
            return dict()

    def _group_services_by_change_enforcement_time(self, services_group_delta : 'GroupOfServicesDelta'):

        services_by_change_enforcement_delay = {}
        for service_name in services_group_delta.services:

            change_enforcement_delay = pd.Timedelta(0, unit = 'ms')
            service_group_delta = services_group_delta.delta_for_service(service_name)

            aspect_sign = service_group_delta.sign_for_aspect(self.service_scaling_infos[service_name].scaled_aspect_name)
            if aspect_sign == -1:
                change_enforcement_delay = self.service_scaling_infos[service_name].booting_duration
            elif aspect_sign == 1:
                change_enforcement_delay = self.service_scaling_infos[service_name].termination_duration


            if not change_enforcement_delay in services_by_change_enforcement_delay:
                services_by_change_enforcement_delay[change_enforcement_delay] = []

            services_by_change_enforcement_delay[change_enforcement_delay].append(service_name)

        return services_by_change_enforcement_delay

    def _enforce_services_of_delta_by_delay(self, services_group_delta : 'GroupOfServicesDelta',
                                            services_by_change_enforcement_delay : dict):

        enforced_deltas_by_delay = dict()

        if len(services_by_change_enforcement_delay) > 0:
            services_by_change_enforcement_delay_sorted = OrderedDict(sorted(services_by_change_enforcement_delay.items(),
                                                                             key = lambda elem: elem[0]))

            for change_enforcement_delay, services_lst in services_by_change_enforcement_delay_sorted.items():

                enforced_services_group_delta, _ = services_group_delta.enforce(services_lst)
                if not enforced_services_group_delta is None:
                    enforced_deltas_by_delay[change_enforcement_delay] = enforced_services_group_delta

        return enforced_deltas_by_delay
