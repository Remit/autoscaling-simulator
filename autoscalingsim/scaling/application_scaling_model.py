import pandas as pd
from collections import OrderedDict
from ..utils.error_check import ErrorChecker
from ..infrastructure_platform.generalized_delta import GeneralizedDelta

class ServiceScalingInfo:

    """
    Wraps scaling information for the service, e.g. how much time is required
    to start a new instance or to terminate the running one.
    """

    def __init__(self,
                 boot_up_delta,
                 termination_ms = 0):

        self.boot_up_ms = pd.Timedelta(boot_up_delta, unit = 'ms')
        self.termination_ms = pd.Timedelta(termination_ms, unit = 'ms')

# TODO: consider removing
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
    A model that contains application scaling-related adjustments descriptions, e.g.
    time required to boot and to terminate the service, and applies them on demand.
    All the services scaling enforcement logic is contained in this model.
    """

    def __init__(self,
                 service_scaling_infos_raw = []):

        self.service_scaling_infos = {}

        for service_scaling_info_raw in service_scaling_infos_raw:

            boot_up_ms = ErrorChecker.key_check_and_load('boot_up_ms', service_scaling_info_raw, self.__class__.__name__)
            termination_ms = ErrorChecker.key_check_and_load('termination_ms', service_scaling_info_raw, self.__class__.__name__)
            ssi = ServiceScalingInfo(pd.Timedelta(boot_up_ms, unit = 'ms'),
                                     pd.Timedelta(termination_ms, unit = 'ms'))

            service_name = ErrorChecker.key_check_and_load('name', service_scaling_info_raw, self.__class__.__name__)

            self.service_scaling_infos[service_name] = ssi

    # TODO: consider removing
    def __iter__(self):
        return ServiceScalingInfoIterator(self)

    # TODO: consider removing
    def get_service_scaling_params(self,
                                   service_name):
        ssi = None
        if service_name in self.service_scaling_infos:
            ssi = self.service_scaling_infos[service_name]

        return ssi

    # TODO: consider removing
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

    def delay(self,
              delta_timestamp : pd.Timestamp,
              generalized_delta : GeneralizedDelta):

        """
        Implements the delay operation on the application level. Returns multiple timestamped
        delayed generalized deltas that each includes both the platform- and application-level delta.
        Applying the delay on the level of entities groups deltas is only possible if
        the bundled container group is already delayed (in_change = False).
        """

        timeline_of_new_deltas = {}
        cur_entity_group_delta = generalized_delta.entity_group_delta.copy()
        if not generalized_delta.container_group_delta.in_change:
            entities_names = cur_entity_group_delta.get_entities()
            # Group entities by their change enforcement time
            entities_by_change_enforcement_delay = {}
            for entity_name in entities_names:
                if not entity_name in self.service_scaling_infos:
                    raise ValueError('No scaling information for entity {} found in {}'.format(entity_name,
                                                                                               self.__class__.__name__))
                change_enforcement_delay = pd.Timedelta(0, unit = 'ms')
                if cur_entity_group_delta.sign < 0:
                    change_enforcement_delay = self.service_scaling_infos[entity_name].boot_up_ms
                elif cur_entity_group_delta.sign > 0:
                    change_enforcement_delay = self.service_scaling_infos[entity_name].termination_ms

                if not change_enforcement_delay in entities_by_change_enforcement_delay:
                    entities_by_change_enforcement_delay[change_enforcement_delay] = [entity_name]
                else:
                    entities_by_change_enforcement_delay[change_enforcement_delay].append(entity_name)

            if len(entities_by_change_enforcement_delay) > 0:
                entities_by_change_enforcement_delay_sorted = OrderedDict(sorted(entities_by_change_enforcement_delay,
                                                                                 lambda elem: elem[0]))

                for change_enforcement_delay, entities_lst in entities_by_change_enforcement_delay_sorted.items():
                    timestamp_after_applying_delay = delta_timestamp + change_enforcement_delay
                    if not cur_entity_group_delta is None:
                        enforced_entity_group_delta, cur_entity_group_delta = cur_entity_group_delta.enforce(entities_lst)
                        if not enforced_entity_group_delta is None:
                            enforced_generalized_delta = GeneralizedDelta(generalized_delta.container_group_delta,
                                                                          enforced_entity_group_delta)
                            timeline_of_new_deltas[timestamp_after_applying_delay] = enforced_generalized_delta

        return timeline_of_new_deltas
