from .service_group.group_of_services import GroupOfServices
from autoscalingsim.infrastructure_platform.node_information.system_resource_usage import SystemResourceUsage

class InNodePlacement:

    def __init__(self, node_info : 'NodeInfo',
                 system_resource_usage : SystemResourceUsage,
                 placed_services : GroupOfServices):

        self.node_info = node_info
        self.system_resource_usage = system_resource_usage
        self.placed_services = placed_services

    def __repr__(self):

        return f'{self.__class__.__name__}( node_info = {self.node_info}, \
                                            system_resource_usage = {self.system_resource_usage}, \
                                            placed_services = {self.placed_services})'

class ServicesPlacement:

    """ The smallest placement unit """

    def __init__(self, node_info : 'NodeInfo', nodes_count : int,
                 single_node_services_state : GroupOfServices):

        self.node_info = node_info
        self.nodes_count = nodes_count
        self.single_node_services_state = single_node_services_state

    def services_that_cannot_be_placed(self, services_to_consider : GroupOfServices):

        collected_state_of_placed_services = self.single_node_services_state.scale_all_service_instances_by(self.nodes_count)
        return services_to_consider % collected_state_of_placed_services

    def __repr__(self):

        return f'{self.__class__.__name__}( node_info = {self.node_info},\
                                            nodes_count = {self.nodes_count},\
                                            services_state = {self.single_node_services_state})'

class Placement:

    def __init__(self, services_placements : list = None):

        self.services_placements = list() if services_placements is None else services_placements
        self._score = None

    def add_services_placement(self, other_services_placement : ServicesPlacement):

        self.services_placements.append(other_services_placement)

    @property
    def score(self):

        return self._score

    @score.setter
    def score(self, score_val):

        self._score = score_val

    def __iter__(self):

        return PlacementIterator(self)

    def __repr__(self):

        return f'{self.__class__.__name__}(services_placements = {self.services_placements})'

class PlacementIterator:

    def __init__(self, placement : Placement):

        self._placement = placement
        self._index = 0

    def __next__(self):

        if self._index < len(self._placement.services_placements):
            placement = self._placement.services_placements[self._index]
            self._index += 1
            return placement

        raise StopIteration
