import pandas as pd
from collections import OrderedDict

from ..utils.error_check import ErrorChecker
from ..utils.state.entity_state.entity_group import EntitiesGroupDelta

class ServiceScalingInfo:

    """
    Wraps scaling information for the service, e.g. how much time is required
    to start a new instance or to terminate the running one.
    """

    def __init__(self,
                 booting_duration : pd.Timedelta,
                 termination_duration : pd.Timedelta = pd.Timedelta(0, unit = 'ms')):

        self.booting_duration = booting_duration
        self.termination_duration = termination_duration

class ApplicationScalingModel:

    """
    A model that contains application scaling-related adjustments descriptions, e.g.
    time required to boot and to terminate the service, and applies them on demand.
    All the services scaling enforcement logic is contained in this model.
    """

    def __init__(self,
                 service_scaling_infos_raw = []):

        self.service_scaling_infos = {}

        for service_scaling_info_raw in service_scaling_infos_raw:

            service_name = ErrorChecker.key_check_and_load('name', service_scaling_info_raw)
            booting_duration = pd.Timedelta(ErrorChecker.key_check_and_load('booting_duration_ms',
                                                                            service_scaling_info_raw,
                                                                            'service',
                                                                            service_name), unit = 'ms')
            termination_duration = pd.Timedelta(ErrorChecker.key_check_and_load('termination_duration_ms',
                                                                                service_scaling_info_raw,
                                                                                'service',
                                                                                service_name), unit = 'ms')
            self.service_scaling_infos[service_name] = ServiceScalingInfo(booting_duration,
                                                                          termination_duration)

    def delay(self,
              entities_group_delta : EntitiesGroupDelta):

        """
        Implements the delay operation on the application level. Returns multiple
        delayed entities deltas indexed by their delays.
        """

        delays_of_enforced_deltas = {}
        if not entities_group_delta is None:
            entities_names = entities_group_delta.get_entities()

            # Group entities by their change enforcement time
            entities_by_change_enforcement_delay = {}
            for entity_name in entities_names:
                if not entity_name in self.service_scaling_infos:
                    raise ValueError('No scaling information for entity {} found in {}'.format(entity_name,
                                                                                               self.__class__.__name__))
                change_enforcement_delay = pd.Timedelta(0, unit = 'ms')
                entity_group_delta = entities_group_delta.get_entity_group_delta(entity_name)

                if entity_group_delta.sign < 0:
                    change_enforcement_delay = self.service_scaling_infos[entity_name].booting_duration
                elif entity_group_delta.sign > 0:
                    change_enforcement_delay = self.service_scaling_infos[entity_name].termination_duration

                if not change_enforcement_delay in entities_by_change_enforcement_delay:
                    entities_by_change_enforcement_delay[change_enforcement_delay] = []

                entities_by_change_enforcement_delay[change_enforcement_delay].append(entity_name)

            if len(entities_by_change_enforcement_delay) > 0:
                entities_by_change_enforcement_delay_sorted = OrderedDict(sorted(entities_by_change_enforcement_delay,
                                                                                 lambda elem: elem[0]))

                for change_enforcement_delay, entities_lst in entities_by_change_enforcement_delay_sorted.items():

                    enforced_entities_group_delta, _ = entities_group_delta.enforce(entities_lst) # all should be enforced by design
                    if not enforced_entities_group_delta is None:
                        delays_of_enforced_deltas[change_enforcement_delay] = enforced_entities_group_delta

        return delays_of_enforced_deltas
