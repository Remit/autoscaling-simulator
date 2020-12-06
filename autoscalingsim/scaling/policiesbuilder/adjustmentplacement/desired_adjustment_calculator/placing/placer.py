from collections import OrderedDict

from .placing_strategy import PlacingStrategy

from autoscalingsim.infrastructure_platform.node_information.system_resource_usage import SystemResourceUsage
from autoscalingsim.desired_state.placement import *
from autoscalingsim.desired_state.service_group.group_of_services import GroupOfServices
from autoscalingsim.scaling.state_reader import StateReader

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

    EXISTING_MIXTURE = 'existing_mixture'
    BALANCED = 'balanced'
    SHARED = 'shared'
    SPECIALIZED = 'specialized'
    SOLE_INSTANCE = 'sole_instance'

    def __init__(self,
                 placement_hint : str,
                 node_for_scaled_services_types : dict,
                 scaled_service_instance_requirements_by_service : dict,
                 reader : StateReader):

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

        placement_options = self.compute_placement_options(services_state,
                                                           region_name,
                                                           dynamic_current_placement,
                                                           dynamic_performance,
                                                           dynamic_resource_utilization)
        nodes_required = {}
        for node_name, placement_options_per_node in placement_options.items():
            node_count_required_per_option = []
            # Computing how many nodes are required to cover the placement option
            for placement_option in placement_options_per_node:

                nodes_required_per_placement = services_state / placement_option.placed_services
                node_count_required_per_option.append(ServicesPlacement(placement_option.node_info,
                                                                        nodes_required_per_placement,
                                                                         placement_option.placed_services))

            if len(node_count_required_per_option) > 0:
                # Selecting the best option for each node
                selected_services_placement = ServicesPlacement(node_info = None,
                                                                nodes_count = float('Inf'),
                                                                services_state = None)
                for considered_services_placement in node_count_required_per_option:
                    if (considered_services_placement.nodes_count > 0) \
                     and (considered_services_placement.nodes_count < selected_services_placement.nodes_count):

                        selected_services_placement = considered_services_placement

                if not selected_services_placement.node_info is None:
                    nodes_required[node_name] = selected_services_placement

        # Correcting the GroupOfServices for each selected option since not all the
        # nodes might be filled equally
        placements = []
        for node_name, placement_option in nodes_required.items():
            #leftover_services_state = services_state % placement_option.services_state
            #remainder_placement = ServicesPlacement(placement_option.node_info,
            #                                        1,
            #                                        leftover_services_state)
            #placement = Placement([remainder_placement])
            #print(f'placement_option.nodes_count: {placement_option.nodes_count}')
            #if placement_option.nodes_count > 1:
            #    placement_option.nodes_count -= 1
            #    placement.add_services_placement(placement_option)

            placement = Placement()
            placement.add_services_placement(placement_option)
            for ep in placement.services_placements:
                print('ep')
                print(ep.nodes_count)
            placements.append(placement)

        return placements

    def compute_placement_options(self,
                                  services_state,
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
        service instance per node for scaled services.
        """

        # Using the cached results if no dynamic information is provided
        if (len(self.cached_placement_options) > 0) and (dynamic_current_placement is None) \
         and (dynamic_performance is None) and (dynamic_resource_utilization is None):
            return self.cached_placement_options

        placement_options = {}
        consider_other_placement_options = False
        if self.placement_hint == self.__class__.EXISTING_MIXTURE:
            placement_options_raw = self.placement_strategies[self.__class__.EXISTING_MIXTURE].place(self,
                                                                                                     region_name,
                                                                                                     dynamic_current_placement,
                                                                                                     dynamic_performance,
                                                                                                     dynamic_resource_utilization)
            placement_options = self._add_placement_options(placement_options,
                                                            placement_options_raw)

            if len(placement_options) == 0:
                consider_other_placement_options = True

        if consider_other_placement_options or (self.placement_hint == self.__class__.BALANCED) or (self.placement_hint == self.__class__.SHARED):

            placement_options_raw = self.placement_strategies[self.__class__.SHARED].place(self,
                                                                                           region_name,
                                                                                           dynamic_performance,
                                                                                           dynamic_resource_utilization)

            if self.placement_hint == self.__class__.BALANCED:
                placement_options_raw = self.placement_strategies[self.__class__.BALANCED].place(self, placement_options_raw)

            placement_options = self._add_placement_options(placement_options,
                                                            placement_options_raw)

            consider_other_placement_options = True

        if consider_other_placement_options or (self.placement_hint == self.__class__.SPECIALIZED):

            placement_options_raw = self.placement_strategies[self.__class__.SPECIALIZED].place(self,
                                                                                                region_name,
                                                                                                dynamic_performance,
                                                                                                dynamic_resource_utilization)
            placement_options = self._add_placement_options(placement_options,
                                                            placement_options_raw)

            consider_other_placement_options = True

        if consider_other_placement_options or (self.placement_hint == self.__class__.SOLE_INSTANCE):
            placement_options_raw = self.placement_strategies[self.__class__.SOLE_INSTANCE].place(self,
                                                                                                  region_name,
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
        Combines scaled services placement options for nodes. The placement
        options were likely received by using different placement strategies (hints).
        """

        for node_name in placement_options_to_add.keys():
            placement_options[node_name] = placement_options.get(node_name, []) + placement_options_to_add[node_name]

        return placement_options
