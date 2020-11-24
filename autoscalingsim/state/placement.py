from .entity_state.service_group import GroupOfServices
from ..infrastructure_platform.system_resource_usage import SystemResourceUsage

class InNodePlacement:

    """
    Wraps the working information about the in-node placement. In case of nodes,
    it will be an in-node placement of services.

    Specifies:
        node type
        capacity taken
        scaled services and their instance counts that can fit into this placement
    """

    def __init__(self,
                 node_info : 'NodeInfo',
                 capacity_taken : SystemResourceUsage,
                 placed_services : GroupOfServices):

        self.node_info = node_info
        self.capacity_taken = capacity_taken
        self.placed_services = placed_services

class ServicesPlacement:

    """
    The smallest placement unit. Wraps a final placement representation for
    a single group of services.
    """

    def __init__(self,
                 node_info : 'NodeInfo',
                 nodes_count : int,
                 services_state : GroupOfServices):

        self.node_info = node_info
        self.nodes_count = nodes_count
        self.services_state = services_state

class Placement:

    """
    Wraps a final placement representation.
    """

    def __init__(self,
                 services_placements : list = []):

        self.services_placements = services_placements
        self.score = None

    def add_services_placement(self,
                               other_services_placement : ServicesPlacement):

        if not isinstance(other_services_placement, ServicesPlacement):
            raise TypeError(f'Wrong type on adding a new services placement: {other_services_placement.__class__.__name__}')

        self.services_placements.append(other_services_placement)

    def __iter__(self):
        return PlacementIterator(self)

class PlacementIterator:

    def __init__(self,
                 placement : Placement):

        self._placement = placement
        self._index = 0

    def __next__(self):

        if self._index < len(self._placement.services_placements):
            placement = self._placement.services_placements[self._index]
            self._index += 1
            return placement

        raise StopIteration
