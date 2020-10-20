from .entity_state.entities_state import EntitiesState

class InContainerPlacement:

    """
    Wraps the working information about the in-container placement. In case of nodes,
    it will be an in-node placement of services.

    Specifies:
        container type
        capacity taken
        scaled entities and their instance counts that can fit into this placement
    """

    def __init__(self,
                 container_type = None,
                 capacity_taken = 0,
                 placed_entities = 0):

        self.container_type = container_type
        self.capacity_taken = capacity_taken
        self.placed_entities = placed_entities

class EntitiesPlacement:

    """
    The smallest placement unit. Wraps a final placement representation for
    a single group of entities.
    """

    def __init__(self,
                 container_name : str,
                 containers_count : int,
                 entities_state : EntitiesState):

        self.container_name = container_name
        self.containers_count = containers_count
        self.entities_state = entities_state

class Placement:

    """
    Wraps a final placement representation.
    """

    def __init__(self,
                 entities_placements : list = []):

        self._placements = entities_placements
        self.score = None

    def add_entities_placement(self,
                               other_entities_placement : EntitiesPlacement):

        if not isinstance(other_entities_placement, EntitiesPlacement):
            raise TypeError('Wrong type on adding a new entities placement: {}'.format(other_entities_placement.__class__.__name__))

        self._placements.append(other_entities_placement)

    def __iter__(self):
        return PlacementIterator(self)

class PlacementIterator:

    """
    Iterates over the entities placements.
    """

    def __init__(self,
                 placement : Placement):

        self._placement = placement
        self._index = 0

    def __next__(self):

        if self._index < len(placement._placements):
            placement = placement._placements[self._index]
            self._index += 1
            return placement

        raise StopIteration
