import pandas as pd
from collections import OrderedDict
from .service_scaling_info import ServiceScalingInfo

from autoscalingsim.utils.error_check import ErrorChecker

class ApplicationScalingModel:

    """ Represents application services horizontal scaling behavior """

    def __init__(self, service_scaling_infos_raw : list, services_scaling_config : dict):

        self.service_scaling_infos = {}
        default_service_conf = services_scaling_config.get('default', None)
        for service_scaling_info_raw in service_scaling_infos_raw:
            service_name = ErrorChecker.key_check_and_load('name', service_scaling_info_raw)
            service_conf = services_scaling_config.get(service_name, default_service_conf)
            scaled_aspect_name = service_conf.scaled_aspect_name if not service_conf is None else 'count'
            self.service_scaling_infos[service_name] = ServiceScalingInfo(service_name, service_scaling_info_raw, scaled_aspect_name)

    def delay(self, services_group_delta : 'GroupOfServicesDelta'):

        """
        Implements the delay operation on the application level. Returns multiple
        delayed services deltas indexed by their delays.
        """

        delays_of_enforced_deltas = {}
        if not services_group_delta is None:
            services_names = services_group_delta.services

            # Group services by their change enforcement time
            services_by_change_enforcement_delay = {}
            for service_name in services_names:
                if not service_name in self.service_scaling_infos:
                    raise ValueError(f'No scaling information for service {service_name} found in {self.__class__.__name__}')
                change_enforcement_delay = pd.Timedelta(0, unit = 'ms')
                service_group_delta = services_group_delta.get_delta_for_service(service_name)

                aspect_sign = service_group_delta.get_aspect_change_sign(self.service_scaling_infos[service_name].scaled_aspect_name)
                if aspect_sign == -1:
                    change_enforcement_delay = self.service_scaling_infos[service_name].booting_duration
                elif aspect_sign == 1:
                    change_enforcement_delay = self.service_scaling_infos[service_name].termination_duration


                if not change_enforcement_delay in services_by_change_enforcement_delay:
                    services_by_change_enforcement_delay[change_enforcement_delay] = []

                services_by_change_enforcement_delay[change_enforcement_delay].append(service_name)

            if len(services_by_change_enforcement_delay) > 0:
                services_by_change_enforcement_delay_sorted = OrderedDict(sorted(services_by_change_enforcement_delay.items(),
                                                                                 key = lambda elem: elem[0]))

                for change_enforcement_delay, services_lst in services_by_change_enforcement_delay_sorted.items():

                    enforced_services_group_delta, _ = services_group_delta.enforce(services_lst) # all should be enforced by design
                    if not enforced_services_group_delta is None:
                        delays_of_enforced_deltas[change_enforcement_delay] = enforced_services_group_delta

        return delays_of_enforced_deltas
