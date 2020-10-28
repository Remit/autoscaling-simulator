from collections import OrderedDict

from .....infrastructure_platform.system_capacity import SystemCapacity
from .....utils.state.placement import *
from .....utils.state.entity_state.entity_group import EntitiesState
from .....utils.state.statemanagers import StateReader

class Placer:

    """
    Proposes services placement options for each node type. These proposals
    are used as constraints by Adjuster, i.e. it can only use the generated
    proposals to search for a needed platform adjustment sufficing to its goal.
    TODO: Placer uses both static and dynamic information to form its proposals.
    In respect to the dynamic information, it can use the runtime utilization and
    performance information to adjust placement space. For instance, a service
    may strive for more memory than it is written in its resource requirements.
    """

    placement_hints = [
        'existing_mixture', # try to use an existig mixture of entities in containers if possible
        'balanced', # attempts to balance capacity consumption in the shared containers
        'shared', # an arbitrary mixture of entities can be placed in the same container
        'specialized', # entities only of a single type are put in the same container
        'sole_instance' # a container is dedicated to a single instance of an entity
    ]

    def __init__(self,
                 placement_hint : str,
                 container_for_scaled_entities_types : dict,
                 scaled_entity_instance_requirements_by_entity : dict,
                 reader : StateReader):

        if not placement_hint in Placer.placement_hints:
            raise ValueError('Adjustment preference {} currently not supported in {}'.format(placement_hint, self.__class__.__name__))

        self.placement_hint = placement_hint
        self.container_for_scaled_entities_types = container_for_scaled_entities_types
        self.scaled_entity_instance_requirements_by_entity = scaled_entity_instance_requirements_by_entity
        self.reader = reader
        self.cached_placement_options = {}
        self.balancing_threshold = 0.05 # TODO: consider providing in config file

    def compute_containers_requirements(self,
                                        entities_state,
                                        region_name : str,
                                        dynamic_current_placement = None,
                                        dynamic_performance = None,
                                        dynamic_resource_utilization = None):

        placement_options = self.compute_placement_options(entities_state,
                                                           region_name,
                                                           dynamic_current_placement,
                                                           dynamic_performance,
                                                           dynamic_resource_utilization)

        containers_required = {}
        for container_name, placement_options_per_container in placement_options.items():
            container_count_required_per_option = []
            # Computing how many containers are required to cover the placement option
            for placement_option in placement_options_per_container:

                containers_required_per_placement = entities_state / placement_option.placed_entities
                container_count_required_per_option.append(EntitiesPlacement(placement_option.container_info,
                                                                             containers_required_per_placement,
                                                                             placement_option.placed_entities))

            if len(container_count_required_per_option) > 0:
                # Selecting the best option for each container
                selected_entities_placement = EntitiesPlacement(container_info = None,
                                                                containers_count = float('Inf'),
                                                                entities_state = None)
                for considered_entities_placement in container_count_required_per_option:
                    if (considered_entities_placement.containers_count > 0) \
                     and (considered_entities_placement.containers_count < selected_entities_placement.containers_count):

                        selected_entities_placement = considered_entities_placement

                if not selected_entities_placement.container_info is None:
                    containers_required[container_name] = selected_entities_placement

        # Correcting the EntitiesState for each selected option since not all the
        # containers might be filled equally
        placements = []
        for container_name, placement_option in containers_required.items():
            leftover_entities_state = entities_state % placement_option.entities_state
            remainder_placement = EntitiesPlacement(placement_option.container_info,
                                                    1,
                                                    leftover_entities_state)
            placement = Placement([remainder_placement])
            if placement_option.containers_count > 1:
                placement_option.containers_count -= 1
                placement.add_entities_placement(placement_option)

            placements.append(placement)

        return placements

    def compute_placement_options(self,
                                  entities_state,
                                  region_name : str,
                                  dynamic_current_placement = None,
                                  dynamic_performance = None,
                                  dynamic_resource_utilization = None):
        """
        Wraps the placement options computation algorithm.
        The algorithm tries to determine the placement options according to the
        the placement hint given. If the placement according to the given hint
        does not succeed, Placer proceeds to the try more relaxed hints to
        generate the in-node placement constraints (options). The 'specialized'
        and the 'sole_instance' options are included whenever any other option
        is provided since the assumption is that the placement must succeed
        at all costs, i.e. if there are no ideal solution, at least some
        placement solution must be given, however bad it is (see 'sole_instance')

        The default last resort for Placer is the 'sole_instance' placement, i.e. single scaled
        entity instance per container for scaled entities.
        """

        # Using the cached results if no dynamic information is provided
        if (len(self.cached_placement_options) > 0) and (dynamic_current_placement is None) \
         and (dynamic_performance is None) and (dynamic_resource_utilization is None):
            return self.cached_placement_options

        placement_options = {}
        consider_other_placement_options = False
        if self.placement_hint == 'existing_mixture':
            placement_options_raw = self._place_existing_mixture(region_name,
                                                                 dynamic_current_placement,
                                                                 dynamic_performance,
                                                                 dynamic_resource_utilization)
            placement_options = self._add_placement_options(placement_options,
                                                            placement_options_raw)

            if len(placement_options) == 0:
                consider_other_placement_options = True

        if consider_other_placement_options or (self.placement_hint == 'balanced') or (self.placement_hint == 'shared'):

            placement_options_raw = self._place_shared(region_name,
                                                       dynamic_performance,
                                                       dynamic_resource_utilization)

            if self.placement_hint == 'balanced':
                placement_options_raw = self._place_balanced(placement_options_raw)

            placement_options = self._add_placement_options(placement_options,
                                                            placement_options_raw)

            consider_other_placement_options = True

        if consider_other_placement_options or (self.placement_hint == 'specialized'):

            placement_options_raw = self._place_specialized(region_name,
                                                            dynamic_performance,
                                                            dynamic_resource_utilization)
            placement_options = self._add_placement_options(placement_options,
                                                            placement_options_raw)

            consider_other_placement_options = True

        if consider_other_placement_options or (self.placement_hint == 'sole_instance'):
            placement_options_raw = self._place_sole_instance(region_name,
                                                              dynamic_performance,
                                                              dynamic_resource_utilization)
            placement_options = self._add_placement_options(placement_options,
                                                            placement_options_raw)

        self.cached_placement_options = placement_options

        return placement_options

    def _add_placement_options(self,
                               placement_options,
                               placement_options_to_add):

        """
        Combines scaled entities placement options for containers. The placement
        options were likely received by using different placement strategies (hints).
        """

        for container_name in placement_options_to_add.keys():
            placement_options[container_name] = placement_options.get(container_name, []) + placement_options_to_add[container_name]

        return placement_options

    def _place_existing_mixture(self,
                                region_name : str,
                                dynamic_current_placement,
                                dynamic_performance = None,
                                dynamic_resource_utilization = None):
        return {}

    def _place_shared(self,
                      region_name : str,
                      dynamic_performance = None,
                      dynamic_resource_utilization = None):

        placement_options = {}

        for provider_name, provider_nodes in self.container_for_scaled_entities_types.items():
            for container_name, container_info in provider_nodes:
                # For each scaled entity compute how much of container does it consume
                container_capacity_taken_by_entity = {}
                for scaled_entity, instance_requirements in self.scaled_entity_instance_requirements_by_entity.items():
                    current_provider = self.reader.get_placement_parameter(scaled_entity,
                                                                           region_name,
                                                                           'provider')# TODO: think of usage?

                    entity_state = EntitiesState(groups_or_aspects = {scaled_entity: {'count': 1}},
                                                 entities_resource_reqs = {scaled_entity: instance_requirements})

                    fits, cap_taken = container_info.entities_require_capacity(entity_state)
                    if fits:
                        container_capacity_taken_by_entity[scaled_entity] = cap_taken

                # Sort in decreasing order of consumed container capacity
                container_capacity_taken_by_entity_sorted = OrderedDict(reversed(sorted(container_capacity_taken_by_entity.items(),
                                                                                        key = lambda elem: elem[1])))

                # Take first in list, and try to add the others to it (maybe with multipliers),
                # then take the next one and try the rest of the sorted list and so on
                placement_options_per_container = []
                considered = []
                for entity_name in container_capacity_taken_by_entity_sorted.keys():

                    further_container_capacity_taken = { entity_name: capacity for entity_name, capacity in container_capacity_taken_by_entity_sorted.items() if not entity_name in considered }
                    single_placement_option_instances = {}
                    cumulative_capacity = SystemCapacity(container_info,
                                                         instance_count = 1)
                    entity_instances_count = 0

                    for entity_name_to_consider, capacity_to_consider in further_container_capacity_taken.items():
                        while not cumulative_capacity.is_exhausted():
                            cumulative_capacity += capacity_to_consider
                            entity_instances_count += 1

                        if cumulative_capacity.is_exhausted():
                            cumulative_capacity -= capacity_to_consider
                            entity_instances_count -= 1

                        entity_placement_option = single_placement_option_instances.get(entity_name_to_consider, {})
                        entity_placement_option['count'] = entity_instances_count
                        single_placement_option_instances[entity_name_to_consider] = entity_placement_option

                    filtered_res_reqs = {}
                    for service_name, resource_req in self.scaled_entity_instance_requirements_by_entity.items():
                        if service_name in single_placement_option_instances:
                            filtered_res_reqs[service_name] = resource_req

                    placement_entities_state = EntitiesState(single_placement_option_instances,
                                                             filtered_res_reqs)

                    single_placement_option = InContainerPlacement(container_info,
                                                                   cumulative_capacity,
                                                                   placement_entities_state)

                    placement_options_per_container.append(single_placement_option)
                    considered.append(entity_name)

                if len(placement_options_per_container) > 0:
                    placement_options[container_name] = placement_options_per_container

        return placement_options

    def _place_balanced(self,
                        shared_placement_options,
                        dynamic_performance = None,
                        dynamic_resource_utilization = None):


        # Select the most balanced options by applying the threshold.
        balanced_placement_options = {}
        for container_name, placement_options_per_container in shared_placement_options.items():
            balanced_placement_options_per_container = []
            best_placement_option_so_far = None

            for single_placement_option in placement_options_per_container:

                if abs(single_placement_option.capacity_taken.collapse() - 1) <= self.balancing_threshold:
                    balanced_placement_options_per_container.append(single_placement_option)

                if abs(single_placement_option.capacity_taken.collapse() - 1) < \
                 abs(best_placement_option_so_far.capacity_taken.collapse() - 1):
                    best_placement_option_so_far = single_placement_option

            # Fallback option: taking the best-balanced solution so far, but not within the balancing threshold
            if (len(balanced_placement_options_per_container) == 0) and (not best_placement_option_so_far is None):
                balanced_placement_options_per_container.append(best_placement_option_so_far)

        return balanced_placement_options_per_container

    def _place_specialized(self,
                           region_name : str,
                           dynamic_performance = None,
                           dynamic_resource_utilization = None):

        placement_options = {}
        for provider_name, provider_nodes in self.container_for_scaled_entities_types.items():
            for container_name, container_info in provider_nodes:
                placement_options_per_container = []

                for scaled_entity, instance_requirements in self.scaled_entity_instance_requirements_by_entity.items():
                    current_provider = self.reader.get_placement_parameter(scaled_entity,
                                                                           region_name,
                                                                           'provider')# TODO: think of usage?

                    entity_state = EntitiesState(groups_or_aspects = {scaled_entity: {'count': 1}},
                                                 entities_resource_reqs = {scaled_entity: instance_requirements})
                    fits, cap_taken = container_info.entities_require_capacity(entity_state)
                    single_placement_option_instances = {}
                    if fits:
                        cumulative_capacity = cap_taken
                        entity_instances_count = 1

                        while not cumulative_capacity.is_exhausted():
                            cumulative_capacity += cap_taken
                            entity_instances_count += 1

                        if cumulative_capacity.is_exhausted():
                            cumulative_capacity -= cap_taken
                            entity_instances_count -= 1

                        entity_placement_option = single_placement_option_instances.get(scaled_entity, {})
                        entity_placement_option['count'] = entity_instances_count
                        single_placement_option_instances[scaled_entity] = entity_placement_option

                        placement_entities_state = EntitiesState(single_placement_option_instances,
                                                                 {scaled_entity: self.scaled_entity_instance_requirements_by_entity[scaled_entity]})

                        single_placement_option = InContainerPlacement(container_info,
                                                                       cumulative_capacity,
                                                                       placement_entities_state)

                        placement_options_per_container.append(single_placement_option)

                if len(placement_options_per_container) > 0:
                    placement_options[container_name] = placement_options_per_container

        return placement_options

    def _place_sole_instance(self,
                             region_name : str,
                             dynamic_performance = None,
                             dynamic_resource_utilization = None):

        placement_options = {}
        for provider_name, provider_nodes in self.container_for_scaled_entities_types.items():
            for container_name, container_info in provider_nodes:
                placement_options_per_container = []

                for scaled_entity, instance_requirements in self.scaled_entity_instance_requirements_by_entity.items():
                    current_provider = self.reader.get_placement_parameter(scaled_entity,
                                                                           region_name,
                                                                           'provider')# TODO: think of usage?

                    entity_state = EntitiesState(groups_or_aspects = {scaled_entity: {'count': 1}},
                                                 entities_resource_reqs = {scaled_entity: instance_requirements})
                    fits, cap_taken = container_info.entities_require_capacity(entity_state)
                    single_placement_option_instances = {}
                    if fits:
                        cumulative_capacity = cap_taken
                        entity_instances_count = 1

                        entity_placement_option = single_placement_option_instances.get(scaled_entity, {})
                        entity_placement_option['count'] = entity_instances_count
                        single_placement_option_instances[scaled_entity] = entity_placement_option

                        placement_entities_state = EntitiesState(single_placement_option_instances,
                                                                 {scaled_entity: self.scaled_entity_instance_requirements_by_entity[scaled_entity]})

                        single_placement_option = InContainerPlacement(container_info,
                                                                       cap_taken,
                                                                       placement_entities_state)

                        placement_options_per_container.append(single_placement_option)

                if len(placement_options_per_container) > 0:
                    placement_options[container_name] = placement_options_per_container

        return placement_options
