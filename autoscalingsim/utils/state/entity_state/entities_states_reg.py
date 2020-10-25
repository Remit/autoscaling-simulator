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
                self.add_state(region_name, value)
            elif isinstance(value, dict) and len(value) > 0:
                self._entities_states_per_region[region_name] = EntitiesState(value)

    def __add__(self,
                other_regionalized_states : 'EntitiesStatesRegionalized'):

        return self._add(other_regionalized_states, 1)

    def __sub__(self,
                other_regionalized_states : 'EntitiesStatesRegionalized'):

        return self._add(other_regionalized_states, -1)

    def _add(self,
             other_regionalized_states : 'EntitiesStatesRegionalized',
             sign : int):

        result = self.copy()
        other_regionalized_states_items = None
        if isinstance(other_regionalized_states, EntitiesStatesRegionalized):
            other_regionalized_states_items = other_regionalized_states
        elif isinstance(other_regionalized_states, dict):
            other_regionalized_states_items = other_regionalized_states.items()
        else:
            raise TypeError('Unknown type of parameter to add to {}: {}'.format(result.__class__,
                                                                                type(other_regionalized_states)))

        for region_name, entities_state in other_regionalized_states_items:
            result.add_state(region_name, entities_state, sign)

        return result

    def add_state(self,
                  region_name : str,
                  entities_state : EntitiesState,
                  sign : int = 1):

        if not isinstance(entities_state, EntitiesState):
            raise TypeError('An attempt to add to {} an operand of a wrong type {}'.format(self.__class__,
                                                                                           type(entities_state)))

        if (not region_name in self._entities_states_per_region) and (sign == 1):
            self._entities_states_per_region[region_name] = entities_state
        else:
            if sign == -1:
                self._entities_states_per_region[region_name] -= entities_state
            elif sign == 1:
                self._entities_states_per_region[region_name] += entities_state

    def __iter__(self):
        return EntitiesStatesIterator(self)

    def get_value(self,
                  region_name : str,
                  entity_name : str):

        if not region_name in self._entities_states_per_region:
            return 0

        return self._entities_states_per_region[region_name].get_value(entity_name)

    def copy(self):

        return EntitiesStatesRegionalized(self._entities_states_per_region.copy())

    def to_delta(self):

        return EntitiesStatesRegionalizedDelta.from_entities_states(self._entities_states_per_region.copy())

    def extract_aspect_representation(self,
                                      scaled_aspect_name : str):

        entities_counts_per_region = {}
        for region_name, entities_state in self._entities_states_per_region.items():
            entities_counts_per_region[region_name] = entities_state.extract_aspect_representation(scaled_aspect_name)

        return entities_counts_per_region

    def extract_countable_representation(self,
                                         conf : dict):

        """
        Used to unify the aggregation scheme both for containers and entities.
        """

        return self.extract_aspect_representation(conf['scaled_aspect_name'])

class EntitiesStatesIterator:

    """
    Allows to iterate over Entities States per region.
    """

    def __init__(self,
                 regionalized_states : EntitiesStatesRegionalized):

        self._regionalized_states = regionalized_states
        self._ordered_index = list(self._regionalized_states._entities_states_per_region.keys())
        self._index = 0

    def __next__(self):

        if self._index < len(self._ordered_index):
            region_name = self._ordered_index[self._index]
            self._index += 1
            return (region_name, self._regionalized_states._entities_states_per_region[region_name])

        raise StopIteration

class EntitiesStatesRegionalizedDelta:

    @staticmethod
    def from_entities_states(entities_states_per_region : dict):

        deltas = {}
        for region_name, entities_state in entities_states_per_region.items():
            deltas[region_name] = entities_state.to_delta()

        return EntitiesStatesRegionalizedDelta(deltas)

    def __init__(self,
                 deltas : dict):

        self.deltas = deltas

    def __add__(self,
                other_delta : 'EntitiesStatesRegionalizedDelta'):

        return self._add(other_delta, 1)

    def __sub__(self,
                other_delta : 'EntitiesStatesRegionalizedDelta'):

        return self._add(other_delta, -1)

    def _add(self,
             other_delta : 'EntitiesStatesRegionalizedDelta',
             sign : int):

        if not isinstance(other_delta, EntitiesStatesRegionalizedDelta):
            raise TypeError('The operand to be added is not of the expected type {}: instead got {}'.format(self.__class__,
                                                                                                            type(other_delta)))

        new_delta = self.copy()
        for region_name in other_delta.deltas:
            if region_name in new_delta.deltas:
                if sign == -1:
                    new_delta.deltas[region_name] -= other_delta.deltas[region_name]
                elif sign == 1:
                    new_delta.deltas[region_name] += other_delta.deltas[region_name]
            else:
                new_delta.deltas[region_name] = other_delta.deltas[region_name]

        return new_delta

    def copy(self):

        return EntitiesStatesRegionalizedDelta(self.deltas.copy())

    def extract_raw_scaling_aspects_changes(self):

        raw_representation = {}
        for region_name, region_delta in self.deltas.items():
            raw_representation[region_name] = region_delta.extract_raw_scaling_aspects_changes()

        return raw_representation
