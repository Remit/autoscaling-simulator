import pandas as pd
from collections import OrderedDict
from abc import ABC

from .requests_processor import RequestsProcessor
from ..entity_state.entity_group import EntitiesGroupDelta, EntitiesState
from ..entity_state.scaling_aspects import ScalingAspect

from ....infrastructure_platform.system_capacity import SystemCapacity
from ....infrastructure_platform.node import NodeInfo
from ....infrastructure_platform.link import NodeGroupLink
from ....infrastructure_platform.utilization import NodeGroupUtilization
from ....load.request import Request
from ....utils.requirements import ResourceRequirements

class ContainerGroup(ABC):

    def __init__(self,
                 container_name : str,
                 provider : str,
                 containers_count : int):

        self.container_name = container_name
        self.provider = provider
        self.containers_count = containers_count

        self.id = id(self)
        self.entities_state = None

    def can_be_coerced(self,
                       container_group : 'ContainerGroup'):

        if not isinstance(container_group, ContainerGroup):
            raise TypeError(f'The provided container group is not of {self.__class__.__name__} type')

        if ((self.container_name == container_group.container_name) and (self.provider == container_group.provider)) \
         or ((self.provider == container_group.provider) and (self.container_name == "any" or container_group.container_name == "any")) \
         or (self.provider == "any" and self.container_name == "any") \
         or (container_group.provider == "any" and container_group.container_name == "any"):
            return True
        else:
            return False

    def get_aspect_value_of_entities_state(self,
                                           entity_name : str,
                                           aspect_name : str):

        if self.entities_state is None:
            return ScalingAspect.get(aspect_name)(0)
        else:
            return self.entities_state.get_aspect_value(entity_name, aspect_name)

class HomogeneousContainerGroupDummy(ContainerGroup):

    """
    Used to describe failures in node groups.
    """

    def __init__(self,
                 container_name : str = None,
                 provider : str = None,
                 containers_count : int = 0):

        super().__init__(container_name, provider, containers_count)

class HomogeneousContainerGroup(ContainerGroup):

    """
    A homogeneous group of containers hosting a specific count of different
    scaled entities (e.g. services). A homogeneous container group is considered
    immutable. It is identified by the hash of its contents that includes
    only the count of scaled entities instances and the container name.
    """

    @classmethod
    def from_conf(cls,
                  group_conf : dict):

        container_info = ErrorChecker.key_check_and_load('container_info', group_conf, self.__class__.__name__)
        containers_count = ErrorChecker.key_check_and_load('containers_count', group_conf, self.__class__.__name__)
        entities_instances_counts = ErrorChecker.key_check_and_load('entities_instances_counts', group_conf, self.__class__.__name__)
        system_requirements = ErrorChecker.key_check_and_load('system_requirements', group_conf, self.__class__.__name__)

        return cls(container_info,
                   containers_count,
                   entities_instances_counts,
                   system_requirements)

    def __init__(self,
                 container_info : NodeInfo,
                 containers_count : int,
                 entities_instances_counts : dict = {},
                 requirements_by_entity : dict = {}):

        super().__init__(container_info.get_name(),
                         container_info.get_provider(),
                         containers_count)

        self.container_info = container_info
        self.utilization = NodeGroupUtilization()
        self.link = NodeGroupLink(self.container_info.latency,
                                  self.containers_count,
                                  self.container_info.network_bandwidth_MBps)

        if isinstance(entities_instances_counts, dict):
            self.entities_state = EntitiesState(entities_instances_counts,
                                                requirements_by_entity)
        elif isinstance(entities_instances_counts, EntitiesState):
            self.entities_state = entities_instances_counts
        else:
            raise TypeError(f'Incorrect type of the entities_instances_counts when creating {self.__class__.__name__}: {entities_instances_counts.__class__.__name__}')

        fits, self.system_capacity = self.container_info.entities_require_capacity(self.entities_state, self.containers_count)
        if not fits:
            raise ValueError('An attempt to place entities on the node group of insufficient capacity')
        self.shared_processor = RequestsProcessor()

    def update_utilization(self,
                           service_name : str,
                           capacity_taken : SystemCapacity,
                           timestamp : pd.Timestamp,
                           averaging_interval : pd.Timedelta):

        self.utilization.update_with_capacity(service_name,
                                              timestamp,
                                              capacity_taken,
                                              averaging_interval)

    def get_utilization(self,
                        service_name : str,
                        resource_name : str,
                        interval : pd.Timedelta):

        return self.utilization.get(service_name, resource_name, interval)

    def get_processed_for_service(self,
                                  service_name : str):

        return self.shared_processor.get_processed_for_service(service_name)

    def system_resources_to_take_from_requirements(self,
                                          res_reqs : ResourceRequirements):

        return self.container_info.system_resources_to_take_from_requirements(res_reqs)

    def system_resources_taken_by_requests(self,
                                           service_name : str,
                                           request_processing_infos : dict):

        reqs_count_by_type = self.shared_processor.get_in_processing_stat(service_name)
        capacity_taken_by_reqs = SystemCapacity(self.container_info, self.containers_count)
        for request_type, request_count in reqs_count_by_type.items():
            cap_taken = self.container_info.system_resources_to_take_from_requirements(request_processing_infos[request_type].resource_requirements) * request_count
            capacity_taken_by_reqs += cap_taken

        return capacity_taken_by_reqs

    def system_resources_taken_by_all_requests(self,
                                               request_processing_infos : dict):

        joint_capacity_taken_by_reqs = SystemCapacity(self.container_info, self.containers_count)
        for service_name in self.shared_processor.get_services_ever_scheduled():
            joint_capacity_taken_by_reqs += self.system_resources_taken_by_requests(service_name, request_processing_infos)

        return joint_capacity_taken_by_reqs

    def step(self,
             time_budget : pd.Timedelta):

        return self.shared_processor.step(time_budget)

    def start_processing(self, req : Request):

        self.shared_processor.start_processing(req)

    def can_schedule_request(self,
                             req : Request,
                             request_processing_infos : dict):

        """
        Checks whether a new request can be scheduled. A new request can be
        scheduled if 1) there are enough free system resources available in the
        node group, and 2) there is at least one service instances available to
        take on the new request.
        """

        system_resources_for_req = self.container_info.system_resources_to_take_from_requirements(request_processing_infos[req.request_type].resource_requirements)
        system_resources_to_be_taken = self.system_resources_taken_by_all_requests(request_processing_infos) \
                                        + self.system_capacity \
                                        + system_resources_for_req

        if (not system_resources_to_be_taken.is_exhausted()) \
         and (self.entities_state.get_entity_count(req.processing_service) \
               > sum( self.shared_processor.get_in_processing_stat(req.processing_service).values() ) ):
            return True
        else:
            return False

    def is_empty(self):

        return self.containers_count == 0

    def shrink(self,
               other_container_group : ContainerGroup):

        """
        This scale down operation results in reducing the containers count
        in the group and may also affect the entities state.
        """

        if not isinstance(other_container_group, ContainerGroup):
            raise TypeError(f'An attempt to subtract unrecognized type from the {self.__class__.__name__}: {other_container_group.__class__.__name__}')

        if self.can_be_coerced(other_container_group):

            if not other_container_group.entities_state is None:
                self.entities_state -= other_container_group.entities_state
            else:
                downsizing_coef = other_container_group.containers_count / self.containers_count
                self.entities_state.downsize_proportionally(downsizing_coef)

            _, self.system_capacity = self.container_info.entities_require_capacity(self.entities_state)

            self.containers_count -= other_container_group.containers_count
            self.link.update_bandwidth(self.containers_count)

    def extract_scaling_aspects(self):

        return self.entities_state.extract_scaling_aspects()

    def add_to_entities_state(self,
                              entities_group_delta : EntitiesGroupDelta):

        self.entities_state += entities_group_delta
        _, self.system_capacity = self.container_info.entities_require_capacity(self.entities_state)

    def nullify_entities_state(self):

        self.entities_state = EntitiesState()
        self.system_capacity = SystemCapacity(self.container_info,
                                              self.containers_count)

    def copy(self):

        return HomogeneousContainerGroup(self.container_info,
                                         self.containers_count,
                                         self.entities_state)

    def to_delta(self,
                 direction : int = 1):

        """
        Converts the container into the *unenforced* GeneralizedDelta representation.
        By default: scale up direction.
        """

        container_group = self.copy()
        container_group.nullify_entities_state()
        container_group_delta = ContainerGroupDelta(container_group,
                                                    sign = direction)

        return GeneralizedDelta(container_group_delta,
                                self.entities_state.to_delta(direction))

    def _count_aspect_soft_adjustment(self,
                                      unmet_changes : dict,
                                      scaled_entity_instance_requirements_by_entity : dict,
                                      container_capacity_taken_by_entity_sorted : dict) -> tuple:

        containers_count_to_consider = self.containers_count
        generalized_deltas = []

        unmet_changes_prev = {}
        while (containers_count_to_consider > 0) and (unmet_changes_prev != unmet_changes):
            unmet_changes_prev = unmet_changes.copy()
            container_capacity_taken = self.system_capacity.copy()

            # Starting with the largest entity and proceeding to the smallest one in terms of
            # capacity requirements. This is made to reduce the capacity fragmentation.
            temp_change = {}
            dynamic_entities_instances_count = self.entities_state.extract_aspect_value('count')

            for entity_name, instance_cap_to_take in container_capacity_taken_by_entity_sorted.items():
                if (entity_name in unmet_changes) and (entity_name in dynamic_entities_instances_count):

                    # Case of adding entities to the existing containers
                    if not entity_name in temp_change:
                        temp_change[entity_name] = 0

                    if (not container_capacity_taken.is_exhausted()) and (unmet_changes[entity_name] > 0):
                        while (unmet_changes[entity_name] - temp_change[entity_name] > 0) and (not container_capacity_taken.is_exhausted()):
                            container_capacity_taken += instance_cap_to_take
                            temp_change[entity_name] += 1

                        if container_capacity_taken.is_exhausted():
                            container_capacity_taken -= instance_cap_to_take
                            temp_change[entity_name] -= 1

                    # Case of removing entities from the existing containers
                    while (unmet_changes[entity_name] - temp_change[entity_name] < 0) and (dynamic_entities_instances_count[entity_name] > 0):
                        container_capacity_taken -= instance_cap_to_take
                        dynamic_entities_instances_count[entity_name] -= 1
                        temp_change[entity_name] -= 1

            #print('soft_adjustment_internal')
            #print(f'temp_change: {temp_change}')
            #print(f'containers_count_to_consider: {containers_count_to_consider}')

            # Trying the same solution temp_accommodation to reduce the amount of iterations by
            # considering whether it can be repeated multiple times
            temp_change = {entity_name: change_val for entity_name, change_val in temp_change.items() if change_val != 0}

            containers_needed = []
            for entity_name, count_in_solution in temp_change.items():
                containers_needed.append(unmet_changes[entity_name] // count_in_solution) # always positive floor

            min_containers_needed = containers_count_to_consider
            if len(containers_needed) > 0:
                min_containers_needed = max(containers_count_to_consider, min(containers_needed))

            containers_count_to_consider -= min_containers_needed

            for entity_name, count_in_solution in temp_change.items():
                unmet_changes[entity_name] -= count_in_solution * max(min_containers_needed, 1)

            container_group_delta = None
            entities_group_delta = None
            temp_change_count = {}
            for entity_name, change_val in temp_change.items():
                temp_change_count[entity_name] = {'count': change_val * max(min_containers_needed, 1)}

            if len(temp_change_count) > 0:
                if container_capacity_taken.collapse() == 0:
                    # scale down for nodes
                    new_entities_instances_counts = self.entities_state.extract_aspect_value('count')

                    container_group = HomogeneousContainerGroup(self.container_info,
                                                                min_containers_needed,
                                                                self.entities_state.copy())

                    # Planning scale down for min_containers_needed of containers
                    container_group_delta = ContainerGroupDelta(container_group,
                                                                sign = -1,
                                                                in_change = True,
                                                                virtual = False)

                else:
                    # scale down/up only for entities, container group remains unchanged
                    container_group_delta = ContainerGroupDelta(self.copy(),
                                                                sign = 1,
                                                                in_change = False,
                                                                virtual = True)

                # Planning scale down for all the entities count change from the solution
                entities_group_delta = EntitiesGroupDelta(temp_change_count,
                                                          in_change = True,
                                                          virtual = False,
                                                          services_reqs = scaled_entity_instance_requirements_by_entity)

                gd = GeneralizedDelta(container_group_delta,
                                      entities_group_delta)

                generalized_deltas.append(gd)

            # Returning generalized deltas (enforced and not enforced) and the unmet changes in entities counts
            unmet_changes = {entity_name: count for entity_name, count in unmet_changes.items() if count != 0}

        return (generalized_deltas, unmet_changes)

    def compute_soft_adjustment(self,
                                scaled_entity_adjustment_in_aspects : dict,
                                scaled_entity_instance_requirements_by_entity : dict) -> tuple:

        """
        Computes the adjustments to the current containers in the Homogeneous
        Container Group in terms of entities added/removed. The computation
        may result in slicing the Group into multiple groups.

        Before adding each particular scaled entity instance,
        it is first checked, whether its requirements can be accomodated by
        the Homogeneous Container Group (spare capacity available).
        Similarly, before removing a scaled entity instance, it is checked,
        whether there are any instances of the given type at all. If there are none
        then there is no effect of such a 'change'.
        """

        container_capacity_taken_by_entity = {}
        for scaled_entity, instance_requirements in scaled_entity_instance_requirements_by_entity.items():
            container_capacity_taken_by_entity[scaled_entity] = self.container_info.system_resources_to_take_from_requirements(instance_requirements)

        # Sort in decreasing order of consumed container capacity:
        # both allocation and deallocation profit more from first trying to
        # place or remove the largest entitites
        container_capacity_taken_by_entity_sorted = OrderedDict(reversed(sorted(container_capacity_taken_by_entity.items(),
                                                                                key = lambda elem: elem[1])))

        unmet_changes_per_aspect = {}
        for entity_name, aspects_change_dict in scaled_entity_adjustment_in_aspects.items():
            for aspect_name, aspect_change_val in aspects_change_dict.items():
                aspect_dict = unmet_changes_per_aspect.get(aspect_name, {})
                aspect_dict[entity_name] = aspect_change_val
                unmet_changes_per_aspect[aspect_name] = aspect_dict

        generalized_deltas = []
        unmet_changes = {}
        for aspect_name, entities_changes_dict in unmet_changes_per_aspect.items():
            method_name = f'_{aspect_name}_aspect_soft_adjustment'
            try:
                aspect_based_generalized_deltas, aspect_based_unmet_changes_res = self.__getattribute__(method_name)(entities_changes_dict,
                                                                                                                     scaled_entity_instance_requirements_by_entity,
                                                                                                                     container_capacity_taken_by_entity_sorted)

                generalized_deltas.extend(aspect_based_generalized_deltas)
                for entity_name, change_val in aspect_based_unmet_changes_res.items():
                    unmet_changes_per_entity = unmet_changes.get(entity_name, {})
                    unmet_changes_per_entity[aspect_name] = change_val
                    unmet_changes[entity_name] = unmet_changes_per_entity

            except AttributeError:
                raise ValueError(f'Support for computing the soft adjustment for the desired aspect type is not implemented: {aspect_name}')

        return (generalized_deltas, unmet_changes)

class ContainerGroupDelta:

    """
    Wraps the container group change and the direction of change, i.e.
    addition or subtraction.
    """

    def __init__(self,
                 container_group : HomogeneousContainerGroup,
                 sign : int = 1,
                 in_change : bool = True,
                 virtual : bool = False):

        if not isinstance(container_group, ContainerGroup):
            raise TypeError(f'The provided parameter is not of {ContainerGroup.__name__} type: {container_group.__class__.__name__}')
        self.container_group = container_group

        if not isinstance(sign, int):
            raise TypeError(f'The provided sign parameters is not of {int.__name__} type: {sign.__class__.__name__}')
        self.sign = sign

        # Signifies whether the delta is just desired (True) or already delayed (False).
        self.in_change = in_change
        # Signifies whether the delta should be considered during the enforcing or not.
        # The aim of 'virtual' property is to keep the connection between the deltas after the enforcement.
        self.virtual = virtual

    def copy(self):

        return ContainerGroupDelta(self.container_group,
                                   self.sign,
                                   self.in_change,
                                   self.virtual)

    def enforce(self):

        return ContainerGroupDelta(self.container_group,
                                   self.sign,
                                   False)

    def get_provider(self):

        return self.container_group.container_info.provider

    def get_container_type(self):

        return self.container_group.container_info.node_type

    def get_container_group_id(self):

        return self.container_group.id

class GeneralizedDelta:

    """
    Wraps the deltas on other abstraction levels such as level of containers and
    the level of scaled entities.
    """

    def __init__(self,
                 container_group_delta : ContainerGroupDelta,
                 entities_group_delta : EntitiesGroupDelta):

        if (not isinstance(container_group_delta, ContainerGroupDelta)) and (not container_group_delta is None):
            raise TypeError(f'The parameter provided for the initialization of {self.__class__.__name__} is not of {ContainerGroupDelta.__name__} type')

        if (not isinstance(entities_group_delta, EntitiesGroupDelta)) and (not entities_group_delta is None):
            raise TypeError(f'The parameter provided for the initialization of {self.__class__.__name__} is not of {EntitiesGroupDelta.__name__} type')

        self.container_group_delta = container_group_delta
        self.entities_group_delta = entities_group_delta
        self.cached_enforcement = {}

    def till_full_enforcement(self,
                              platform_scaling_model,
                              application_scaling_model,
                              delta_timestamp : pd.Timestamp):

        """
        Computes the time required for the enforcement to finish at all levels.
        Makes the enforcement underneath.
        """

        new_deltas = self.enforce(platform_scaling_model,
                                  application_scaling_model,
                                  delta_timestamp)

        time_until_enforcement = pd.Timedelta(0, unit = 'ms')
        if len(new_deltas) > 0:
            time_until_enforcement = max(list(new_deltas.keys())) - delta_timestamp

        return time_until_enforcement

    def enforce(self,
                platform_scaling_model,
                application_scaling_model,
                delta_timestamp : pd.Timestamp):

        """
        Forms enforced deltas for both parts of the generalized delta and returns
        these as timelines. The enforcement takes into account the sequence of
        scaling actions. On scale down, all the entities should terminate first.
        On scale up, the container group should boot first.

        In addition, it caches the enforcement on first computation since
        the preliminary till_full_enforcement method requires it.
        """

        if delta_timestamp in self.cached_enforcement:
            return self.cached_enforcement[delta_timestamp]

        self.cached_enforcement = {}
        new_deltas = {}
        if self.container_group_delta.in_change and (not self.container_group_delta.virtual):
            delay_from_containers = pd.Timedelta(0, unit = 'ms')
            max_entity_delay = pd.Timedelta(0, unit = 'ms')
            container_group_delta_virtual = None

            container_group_delay, container_group_delta = platform_scaling_model.delay(self.container_group_delta)
            entities_groups_deltas_by_delays = application_scaling_model.delay(self.entities_group_delta)

            if self.container_group_delta.sign < 0:
                # Adjusting params for the graceful scale down
                if len(entities_groups_deltas_by_delays) > 0:
                    max_entity_delay = max(list(entities_groups_deltas_by_delays.keys()))
                container_group_delta_virtual = self.container_group_delta.copy()
            elif self.container_group_delta.sign > 0:
                # Adjusting params for scale up
                delay_from_containers = container_group_delay
                container_group_delta_virtual = container_group_delta.copy()

            # Delta for containers
            new_timestamp = delta_timestamp + max_entity_delay + container_group_delay
            if not new_timestamp in new_deltas:
                new_deltas[new_timestamp] = []
            new_deltas[new_timestamp].append(GeneralizedDelta(container_group_delta,
                                                              None))

            # Deltas for entities -- connecting them to the corresponding containers
            for delay, entities_group_delta in entities_groups_deltas_by_delays.items():
                new_timestamp = delta_timestamp + delay + delay_from_containers
                if not new_timestamp in new_deltas:
                    new_deltas[new_timestamp] = []

                container_group_delta_virtual.virtual = True
                new_deltas[new_timestamp].append(GeneralizedDelta(container_group_delta_virtual,
                                                                  entities_group_delta))

        self.cached_enforcement[delta_timestamp] = new_deltas

        return new_deltas
