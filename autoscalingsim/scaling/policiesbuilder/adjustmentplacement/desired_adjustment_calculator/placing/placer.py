import collections

from .placing_strategy import PlacingStrategy

from autoscalingsim.infrastructure_platform.node_information.system_resource_usage import SystemResourceUsage
from autoscalingsim.desired_state.placement import *
from autoscalingsim.desired_state.service_group.group_of_services import GroupOfServices
from autoscalingsim.scaling.state_reader import StateReader

class Placer:

    """ Proposes services placement options for each node type. These proposals are used as constraints. """

    EXISTING_MIXTURE = 'existing_mixture'
    BALANCED = 'balanced'
    SHARED = 'shared'
    SPECIALIZED = 'specialized'
    SOLE_INSTANCE = 'sole_instance'

    def __init__(self, placement_hint : str, node_for_scaled_services_types : dict,
                 scaled_service_instance_requirements_by_service : dict, reader : StateReader):

        self.placement_hint = placement_hint
        self.node_for_scaled_services_types = node_for_scaled_services_types
        self.scaled_service_instance_requirements_by_service = scaled_service_instance_requirements_by_service
        self.reader = reader
        self.cached_placement_options = {}
        self.balancing_threshold = 0.05 # TODO: consider providing in config file

        self.placement_strategies = { strategy_name : strategy_cls() for strategy_name, strategy_cls in PlacingStrategy.items() }

    def compute_nodes_requirements(self, services_state, region_name : str,
                                   dynamic_current_placement = None, dynamic_performance = None,
                                   dynamic_resource_utilization = None):

        placement_options = self._produce_placement_options(services_state,
                                                            region_name,
                                                            dynamic_current_placement,
                                                            dynamic_performance,
                                                            dynamic_resource_utilization)

        nodes_required = collections.defaultdict(ServicesPlacement)
        for node_name, placement_options_per_node in placement_options.items():

            node_count_required_per_option = self._compute_node_count_to_cover_the_placement(services_state, placement_options_per_node)

            best_option = self._select_best_option_for_the_node_type(node_count_required_per_option)
            if not best_option is None:
                nodes_required[node_name] = best_option

        return self._complement_placement_options_with_remainders_if_needed(nodes_required, services_state)

    def _produce_placement_options(self, services_state, region_name : str,
                                   dynamic_current_placement = None,
                                   dynamic_performance = None,
                                   dynamic_resource_utilization = None):
        """
        The algorithm tries to determine the placement options according to the
        the placement hint given. If the placement according to the given hint
        does not succeed, Placer proceeds to the try more relaxed hints to
        generate the in-node placement constraints (options).
        """

        if self._can_cached_result_be_used(dynamic_current_placement, dynamic_performance, dynamic_resource_utilization):
            return self.cached_placement_options

        placement_options = collections.defaultdict(list)
        consider_other_placement_options = False
        if self.placement_hint == self.__class__.EXISTING_MIXTURE:
            placement_options_raw = self.placement_strategies[self.__class__.EXISTING_MIXTURE].place(self,
                                                                                                     region_name,
                                                                                                     dynamic_current_placement,
                                                                                                     dynamic_performance,
                                                                                                     dynamic_resource_utilization)
            self._enrich_placement_options(placement_options, placement_options_raw)

            if len(placement_options) == 0:
                consider_other_placement_options = True

        if consider_other_placement_options or (self.placement_hint == self.__class__.BALANCED) or (self.placement_hint == self.__class__.SHARED):

            placement_options_raw = self.placement_strategies[self.__class__.SHARED].place(self,
                                                                                           region_name,
                                                                                           dynamic_performance,
                                                                                           dynamic_resource_utilization)

            if self.placement_hint == self.__class__.BALANCED:
                placement_options_raw = self.placement_strategies[self.__class__.BALANCED].place(self, placement_options_raw)

            self._enrich_placement_options(placement_options, placement_options_raw)

            consider_other_placement_options = True

        if consider_other_placement_options or (self.placement_hint == self.__class__.SPECIALIZED):

            placement_options_raw = self.placement_strategies[self.__class__.SPECIALIZED].place(self,
                                                                                                region_name,
                                                                                                dynamic_performance,
                                                                                                dynamic_resource_utilization)
            self._enrich_placement_options(placement_options, placement_options_raw)

            consider_other_placement_options = True

        if consider_other_placement_options or (self.placement_hint == self.__class__.SOLE_INSTANCE):
            placement_options_raw = self.placement_strategies[self.__class__.SOLE_INSTANCE].place(self,
                                                                                                  region_name,
                                                                                                  dynamic_performance,
                                                                                                  dynamic_resource_utilization)
            self._enrich_placement_options(placement_options, placement_options_raw)

        self.cached_placement_options = placement_options

        return placement_options

    def _can_cached_result_be_used(self, dynamic_current_placement, dynamic_performance, dynamic_resource_utilization):

        return len(self.cached_placement_options) > 0 and dynamic_current_placement is None \
                    and dynamic_performance is None and dynamic_resource_utilization is None

    def _enrich_placement_options(self, placement_options, placement_options_to_add):

        for node_name in placement_options_to_add.keys():
            placement_options[node_name].extend(placement_options_to_add[node_name])

    def _compute_node_count_to_cover_the_placement(self, services_state, placement_options_per_node):

        result = list()
        for placement_option in placement_options_per_node:

            nodes_required_per_placement = services_state / placement_option.placed_services
            result.append(ServicesPlacement(placement_option.node_info,
                                            nodes_required_per_placement,
                                            placement_option.placed_services))

        return result

    def _select_best_option_for_the_node_type(self, node_count_required_per_option):

        result = ServicesPlacement(node_info = None, nodes_count = float('Inf'), services_state = None)

        for considered_services_placement in node_count_required_per_option:
            if (considered_services_placement.nodes_count > 0) \
             and (considered_services_placement.nodes_count < result.nodes_count):

                result = considered_services_placement

        return result if not result.node_info is None else None

    def _complement_placement_options_with_remainders_if_needed(self, nodes_required, services_state):

        placements = list()
        for node_name, placement_option in nodes_required.items():
            leftover_services_state = placement_option.services_that_cannot_be_placed(services_state)
            if leftover_services_state.is_empty:
                placements.append(Placement([placement_option]))

            else:
                remainder_placement = ServicesPlacement(placement_option.node_info, 1, leftover_services_state)

                if placement_option.nodes_count <= 1:
                    placements.append(Placement([remainder_placement]))
                else:
                    placement_option.nodes_count -= 1
                    placements.append(Placement([placement_option, remainder_placement]))

        return placements
