from .entities_state import EntitiesState

class EntitiesStatesRegionalized:

    """
    Wraps multiple Entities States each belonging to a separate region.
    """

    def __init__(self,
                 entities_states_per_region : dict):

        self._entities_states_per_region = {}
        for region_name, value in entities_states_per_region.items():
            if isinstance(value, EntitiesState):
                self.add_state(value)
            elif isinstance(value, dict) and len(value) > 0:
                self._entities_states_per_region[region_name] = EntitiesState(value)

    def __add__(self,
                other_regionalized_states):

        result = self.copy()
        if not isinstance(other_regionalized_states, result.__class__):
            if not isinstance(other_regionalized_states, dict):
                raise TypeError('Unknown type of parameter to add to {}: {}'.format(result.__class__.__name__,
                                                                                    other_regionalized_states.__class__.__name__))
            for region_name, state in other_regionalized_states.items():
                if not isinstance(state, EntitiesState):
                    raise TypeError('Unknown type of parameters in dict: {}'.format(state.__class__.__name__))

                result.add_state(region_name, state)

        else:
            for region_name, state in other_regionalized_states.items():
                result.add_state(region_name, state)

        return result

    def __iter__(self):
        return EntitiesStatesIterator(self)

    def add_state(self,
                  region_name : str,
                  entities_state : EntitiesState):

        if not isinstance(entities_state, EntitiesState):
            raise TypeError('An attempt to add to {} an operand of a wrong type {}'.format(self.__class__.__name__,
                                                                                           entities_state.__class__.__name__))

        if not region_name in self._entities_states_per_region:
            self._entities_states_per_region[region_name] = EntitiesState()
        self._entities_states_per_region[region_name] += entities_state

    def copy(self):

        return EntitiesStatesRegionalized(self._entities_states_per_region.copy())

class EntitiesStatesIterator:

    """
    Allows to iterate over Entities States per region.
    """

    def __init__(self,
                 regionalized_states : EntitiesStatesRegionalized):

        self._regionalized_states = regionalized_states
        self._ordered_index = list(self._regionalized_states.keys())
        self._index = 0

    def __next__(self):

        if self._index < len(self._ordered_index):
            region_name = self._ordered_index[self._index]
            self._index += 1
            return (region_name, self._regionalized_states[region_name])

        raise StopIteration
