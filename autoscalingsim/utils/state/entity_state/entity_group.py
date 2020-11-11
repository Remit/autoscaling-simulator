import numbers
import collections
import numpy as np

from .scaling_aspects import ScalingAspect, ScalingAspectDelta

from ...error_check import ErrorChecker
from ...requirements import ResourceRequirements

class EntityGroup:

    """
    Wraps the state of multiple scaled entities. The state is divided into
    the static and dynamic parts. The static part is the current count of
    entities. The dynamic part is the count of booting/terminating entities.
    """

    def __init__(self,
                 entity_name : str,
                 entity_resource_reqs : ResourceRequirements,
                 aspects_vals = {'count': 1}):

        """
        If just a single integer is provided as a param aspects_vals, then it is
        considered to be the count of entities.
        """

        self.entity_name = entity_name
        self.scaling_aspects = {}

        if not isinstance(entity_resource_reqs, ResourceRequirements):
            raise TypeError(f'Unexpected type for entity resource requirements: {entity_resource_reqs.__class__.__name__}')
        self.entity_resource_reqs = entity_resource_reqs

        if isinstance(aspects_vals, collections.Mapping):
            for aspect_name, aspect_value in aspects_vals.items():
                if isinstance(aspect_value, ScalingAspect):
                    self.scaling_aspects[aspect_name] = aspect_value
                elif isinstance(aspect_value, numbers.Number):
                    self.scaling_aspects[aspect_name] = ScalingAspect.get(aspect_name)(aspect_value)
                else:
                    raise TypeError(f'Unexpected type of scaling aspects values to initialize the {self.__class__.__name__}')
        else:
            raise TypeError(f'Unexpected type of scaling aspects dictionary to initialize the {self.__class__.__name__}')

    def __add__(self,
                other_group_or_delta):

        return self._add(other_group_or_delta, 1)

    def __sub__(self,
                other_group_or_delta):

        return self._add(other_group_or_delta, -1)

    def _add(self,
             other_group_or_delta,
             sign : int):

        to_add = None
        if isinstance(other_group_or_delta, EntityGroup):
            to_add = other_group_or_delta.scaling_aspects
        elif isinstance(other_group_or_delta, EntityGroupDelta):
            to_add = other_group_or_delta.aspects_deltas
        else:
            raise TypeError(f'Incorrect type of operand to _add to {self.__class__.__name__}: {other_group_or_delta.__class__.__name__}')

        if self.entity_name != other_group_or_delta.entity_name:
            raise ValueError(f'Non-matching names of operands to _add: {self.entity_name} and {other_group_or_delta.entity_name}')

        new_group = self.copy()
        for aspect_name in to_add:
            if aspect_name in new_group.scaling_aspects:
                if sign == -1:
                    new_group.scaling_aspects[aspect_name] -= to_add[aspect_name]
                elif sign == 1:
                    new_group.scaling_aspects[aspect_name] += to_add[aspect_name]
            elif sign == 1:
                new_group.scaling_aspects[aspect_name] = ScalingAspect.get(aspect_name)()
                new_group.scaling_aspects[aspect_name] += to_add[aspect_name]

        return new_group

    def __mul__(self,
                multiplier : int):

        if not isinstance(multiplier, int):
            raise TypeError(f'Incorrect type of mulitiplier to multiply {self.__class__.__name__} by: {multiplier.__class__.__name__}')

        new_aspects = self.scaling_aspects.copy()
        for aspect_name, aspect in self.scaling_aspects.items():
            new_aspects[aspect_name] *= multiplier

        return EntityGroup(self.entity_name, new_aspects)

    def __floordiv__(self,
                     other_entity_group : 'EntityGroup'):

        """
        Returns the list of values. Each value corresponds to a scaling aspect and
        signifies how many times does the scaling aspect of the current group covers
        the corresponding scaling aspect of the parameter.
        """

        if not isinstance(other_entity_group, EntityGroup):
            raise TypeError(f'An attempt to floor-divide by an unknown type {other_entity_group.__class__.__name__}')

        division_results = []
        for aspect_name, aspect_value in self.scaling_aspects.items():
            if aspect_name in other_entity_group.scaling_aspects:
                division_results.append((aspect_value // other_entity_group.scaling_aspects[aspect_name]).get_value())

        return division_results

    def __mod__(self,
                other_entity_group):

        if not isinstance(other_entity_group, self.__class__):
            raise TypeError(f'Incorrect type of operand to take modulo of {self.__class__.__name__}: {other_entity_group.__class__.__name__}')

        if self.entity_name != other_entity_group.entity_name:
            raise ValueError(f'Non-matching names of EntityGroups to take modulo: {self.entity_name} and {other_entity_group.entity_name}')

        new_aspects = self.scaling_aspects.copy()
        for aspect_name, aspect in self.scaling_aspects.items():
            if aspect_name in other_entity_group.scaling_aspects:
                new_aspects[aspect_name] %= other_entity_group.scaling_aspects[aspect_name]

        return EntityGroup(self.entity_name, new_aspects)

    def copy(self):

        return EntityGroup(self.entity_name,
                           self.entity_resource_reqs,
                           self.scaling_aspects.copy())

    def to_delta(self,
                 direction = 1):

        """
        Converts the current EntityGroup into its delta representation.
        Assumes scale up direction.
        """

        return EntityGroupDelta.from_group(self, direction)

    def update_aspect(self,
                      aspect : ScalingAspect):

        if not aspect.name in self.scaling_aspects:
            raise ValueError(f'Unexpected aspect for an update: {aspect.name}')

        self.scaling_aspects[aspect.name] = aspect

    def get_aspect_value(self,
                         aspect_name : str):

        if not aspect_name in self.scaling_aspects:
            raise ValueError(f'Unexpected aspect for an update: {aspect_name}')

        return self.scaling_aspects[aspect_name].copy()

    def get_resource_requirements(self):

        return self.entity_resource_reqs

class EntityGroupDelta:

    """
    Wraps the entity group change and the direction of change, i.e. addition
    or subtraction.
    """

    @staticmethod
    def from_group(entity_group : EntityGroup,
                   sign = 1):

        if not isinstance(sign, int):
            raise TypeError(f'The provided sign parameters is not of {int.__name__} type: {sign.__class__.__name__}')

        if not isinstance(entity_group, EntityGroup):
            raise TypeError(f'The provided argument is not of EntityGroup type: {entity_group.__class__.__name__}')

        aspects_vals_raw_numbers = {}
        for aspect_name, aspect_value in entity_group.scaling_aspects.items():
            aspects_vals_raw_numbers[aspect_name] = sign * aspect_value.get_value()

        return EntityGroupDelta(entity_group.entity_name,
                                aspects_vals_raw_numbers,
                                entity_group.entity_resource_reqs)

    def __init__(self,
                 entity_name : str,
                 aspects_vals : dict,
                 entity_resource_reqs : ResourceRequirements):

        self.entity_name = entity_name
        if not isinstance(entity_resource_reqs, ResourceRequirements):
            raise TypeError(f'Unexpected type for entity resource requirements when initializing {self.__class__.__name__}: {entity_resource_reqs.__class__.__name__}')
        self.entity_resource_reqs = entity_resource_reqs
        self.aspects_deltas = {}
        for aspect_name, aspect_value in aspects_vals.items():
            if isinstance(aspect_value, ScalingAspect):
                self.aspects_deltas[aspect_name] = ScalingAspectDelta(aspect_value)
            elif isinstance(aspect_value, numbers.Number):
                self.aspects_deltas[aspect_name] = ScalingAspectDelta(ScalingAspect.get(aspect_name)(abs(aspect_value)),
                                                                      int(np.sign(aspect_value)))
            else:
                raise TypeError(f'Unexpected type of scaling aspects values to initialize the {self.__class__.__name__}')

    def __add__(self,
                other_delta : 'EntityGroupDelta'):

        return self._add(other_delta, 1)

    def __sub__(self,
                other_delta : 'EntityGroupDelta'):

        return self._add(other_delta, -1)

    def _add(self,
             other_delta : 'EntityGroupDelta',
             sign : int):

        if not isinstance(other_delta, EntityGroupDelta):
            raise TypeError(f'The operand to be added is not of the expected type {self.__class__.__name__}, got {other_delta.__class__.__name__}')

        if self.entity_name != other_delta.entity_name:
            raise ValueError(f'An attempt to add {self.__class__.__name__} with different names: {self.entity_name} and {other_delta.entity_name}')

        new_delta = self.copy()
        for aspect_name in other_delta.aspects_deltas:
            if aspect_name in new_delta.aspects_deltas:
                if sign == -1:
                    new_delta.aspects_deltas[aspect_name] -= other_delta.aspects_deltas[aspect_name]
                elif sign == 1:
                    new_delta.aspects_deltas[aspect_name] += other_delta.aspects_deltas[aspect_name]
            else:
                new_delta.aspects_deltas[aspect_name] = other_delta.aspects_deltas[aspect_name]

        return new_delta

    def copy(self):

        return EntityGroupDelta(self.entity_name,
                                self.to_raw_change(),
                                self.entity_resource_reqs)

    def to_raw_change(self):

        aspects_vals_raw_numbers = {}
        for aspect_name, aspect_delta in self.aspects_deltas.items():
            aspects_vals_raw_numbers[aspect_name] = aspect_delta.to_raw_change()

        return aspects_vals_raw_numbers

    def to_entity_group(self):

        return EntityGroup(self.entity_name,
                           self.entity_resource_reqs,
                           self.to_raw_change())

    def get_aspect_change_sign(self,
                               scaled_aspect_name : str):

        if scaled_aspect_name in self.aspects_deltas:
            return self.aspects_deltas[scaled_aspect_name].sign
        else:
            raise ValueError(f'Aspect {scaled_aspect_name} not found in {self.__class__.__name__}')

class EntitiesState:

    """
    Wraps the state of entities on a particular container group.
    """

    def __init__(self,
                 groups_or_aspects : dict = {},
                 entities_resource_reqs : dict = {}):

        self.entities_groups = {}
        if len(groups_or_aspects) > 0:
            for entity_name, group_or_aspects_dict in groups_or_aspects.items():
                if isinstance(group_or_aspects_dict, EntityGroup):
                    self.entities_groups[entity_name] = group_or_aspects_dict
                elif isinstance(groups_or_aspects, collections.Mapping):
                    if len(entities_resource_reqs) == 0:
                        raise ValueError(f'No resource requirements provided for the initialization of {self.__class__.__name__}')
                    self.entities_groups[entity_name] = EntityGroup(entity_name,
                                                                    entities_resource_reqs[entity_name],
                                                                    group_or_aspects_dict)
                else:
                    raise TypeError(f'Unknown type of the init parameter: {groups_or_aspects.__class__.__name__}')

    def get_entities_counts(self):

        entities_counts = {}
        for entity_name, group in self.entities_groups.items():
            entities_counts[entity_name] = group.get_aspect_value('count').get_value()

        return entities_counts

    def get_entities_requirements(self):

        reqs_by_entity = {}
        for entity_name, group in self.entities_groups.items():
            reqs_by_entity[entity_name] = group.get_resource_requirements()

        return reqs_by_entity

    def copy(self):

        return EntitiesState(self.entities_groups.copy())

    def __add__(self,
                entities_state_or_delta):

        return self._add(entities_state_or_delta, 1)

    def __sub__(self,
                entities_state_or_delta):

        return self._add(entities_state_or_delta, -1)

    def _add(self,
             entities_state_or_delta,
             sign : int):

        """
        Adds an argument to the current Entities State taking sign into account.
        The argument can be either of an EntitiesGroupDelta class or of an EntitiesState class.
        Acts as a common part for __add__ and __sub__.
        """

        new_groups = {}
        if isinstance(entities_state_or_delta, EntitiesGroupDelta):
            if entities_state_or_delta.in_change:
                raise ValueError('Cannot add the delta that is still in change to the current entities state')
            else:
                for entity_name, entity_delta in entities_state_or_delta.deltas.items():
                    if entity_name in self.entities_groups:
                        if sign == -1:
                            new_groups[entity_name] = self.entities_groups[entity_name] - entity_delta
                        elif sign == 1:
                            new_groups[entity_name] = self.entities_groups[entity_name] + entity_delta
                    elif sign == 1:
                        new_groups[entity_name] = entity_delta.to_entity_group()

        elif isinstance(entities_state_or_delta, EntitiesState):
            for entity_name, entity_group_to_add in entities_state_or_delta.entities_groups.items():
                if entity_name in self.entities_groups:
                    if sign == -1:
                        new_groups[entity_name] = self.entities_groups[entity_name] - entity_group_to_add
                    elif sign == 1:
                        new_groups[entity_name] = self.entities_groups[entity_name] + entity_group_to_add
                elif sign == 1:
                    new_groups[entity_name] = entity_group_to_add
        else:
            raise TypeError(f'An attempt to add the operand of type {entities_to_add.__class__.__name__} to the {self.__class__.__name__} when expecting type EntitiesGroupDelta or EntitiesState')
        return EntitiesState(new_groups)

    def __truediv__(self,
                    other_entities_state : 'EntitiesState'):

        """
        Defines the division of one entities state by another. Allows to figure out, how
        many replicas of the argument state can be hosted within the original
        state fully. The remainder can be calculated with __mod__.
        """

        if not isinstance(other_entities_state, self.__class__):
            raise TypeError(f'Incorrect type of operand to divide {self.__class__.__name__} by: {other_entities_state.__class__.__name__}')

        division_result_raw = {}
        for entity_name, entity_group in self.entities_groups.items():
            if not entity_name in other_entities_state.entities_groups:
                return 0
            else:
                division_result_raw[entity_name] = min(entity_group // other_entities_state.entities_groups[entity_name])

        return min(division_result_raw.values())

    def __mod__(self,
                other_entities_state : 'EntitiesState'):

        """
        Computes the remainder entities state that is only partially covered by the
        current entities state. Complements the __truediv__ defined above.
        """

        if not isinstance(other_entities_state, self.__class__):
            raise TypeError(f'Incorrect type of operand to take {self.__class__.__name__} modulo: {other_entities_state.__class__.__name__}')

        remainder_groups = {}
        for entity_name, entity_group in self.entities_groups.items():
            if entity_group in other_entities_state.entities_groups:
                remainder_groups[entity_name] = entity_group % other_entities_state.entities_groups[entity_name]
            else:
                remainder_groups[entity_name] = entity_group

        return EntitiesState(remainder_groups)

    def to_delta(self,
                 direction : int = 1):

        """
        Converts the current EntitiesState into its GeneralizedDelta representation.
        Assumes scale up direction for every EntityGroup.
        """

        delta = EntitiesGroupDelta()
        for group in self.entities_groups.values():
            delta.add(group.to_delta(direction))

        return delta

    def extract_scaling_aspects(self):

        aspect_vals_dict = {}
        for entity_name, entity_group in self.entities_groups.items():
            aspect_vals_dict[entity_name] = entity_group.scaling_aspects

        return aspect_vals_dict

    def extract_aspect_representation(self,
                                      aspect_name : str):

        aspect_vals_dict = {}
        for entity_name, entity_group in self.entities_groups.items():
            aspect_vals_dict[entity_name] = entity_group.get_aspect_value(aspect_name)

        return aspect_vals_dict

    def extract_aspect_value(self,
                             aspect_name : str):

        aspect_vals_dict = {}
        for entity_name, entity_group in self.entities_groups.items():
            aspect_vals_dict[entity_name] = entity_group.get_aspect_value(aspect_name).get_value()

        return aspect_vals_dict

    def get_aspect_value(self,
                         entity_name : str,
                         aspect_name : str):

        if entity_name in self.entities_groups:
            return self.entities_groups[entity_name].get_aspect_value(aspect_name)
        else:
            return 0

    def get_entity_count(self,
                         entity_name : str):

        return self.get_aspect_value(entity_name, 'count').get_value()

    def get_entity_resource_requirements(self,
                                         entity_name : str):

        if entity_name in self.entities_groups:
            return self.entities_groups[entity_name].get_resource_requirements()
        else:
            raise ValueError(f'An attempt to get the resource requirements for an unknown entity {entity_name}')

class EntitiesGroupDelta:

    """
    Wraps multiple EntityGroupDelta distinguished by the entity.
    """

    @classmethod
    def from_entity_group_deltas(cls : type,
                                 deltas : dict,
                                 in_change : bool = True,
                                 virtual : bool = False):

        entities_group_delta = cls({}, in_change, virtual)
        entities_group_delta.deltas = deltas

        return entities_group_delta

    def __init__(self,
                 aspects_vals_per_entity : dict = {},
                 in_change : bool = True,
                 virtual : bool = False,
                 services_reqs : dict = {}):

        self.deltas = {}
        for entity_name, aspects_vals in aspects_vals_per_entity.items():

            if entity_name not in services_reqs:
                raise ValueError(f'Resource requirements for entity {entity_name} were not provided')

            self.deltas[entity_name] = EntityGroupDelta(entity_name,
                                                        aspects_vals,
                                                        services_reqs[entity_name])

        # Signifies whether the delta should be considered during the enforcing or not.
        # The aim of 'virtual' property is to keep the connection between the deltas after the enforcement.
        self.virtual = virtual
        self.in_change = in_change

    def to_entities_state(self):

        groups = {}
        for entity_name, delta in self.deltas.items():
            groups[entity_name] = delta.to_entity_group()

        return EntitiesState(groups)

    def get_entities(self):

        return list(self.deltas.keys())

    def get_entity_group_delta(self,
                               entity_name : str):

        if not entity_name in self.deltas:
            raise ValueError(f'No entity group delta for entity name {entity_name} found')

        return self.deltas[entity_name]

    def __add__(self,
                other_delta : 'EntitiesGroupDelta'):

        return self._add(other_delta, 1)

    def __sub__(self,
                other_delta : 'EntitiesGroupDelta'):

        return self._add(other_delta, -1)

    def _add(self,
             other_delta : 'EntitiesGroupDelta',
             sign : int):

        if not isinstance(other_delta, EntitiesGroupDelta):
            raise TypeError(f'The operand to be added is not of the expected type {self.__class__.__name__}, got {other_delta.__class__.__name__}')

        if self.in_change != other_delta.in_change:
            raise ValueError('Addition operands differ by the in_change status')

        new_delta = self.copy()
        for entity_name in other_delta.deltas:
            if entity_name in new_delta.deltas:
                if sign == -1:
                    new_delta.deltas[entity_name] -= other_delta.deltas[entity_name]
                elif sign == 1:
                    new_delta.deltas[entity_name] += other_delta.deltas[entity_name]
            else:
                new_delta.deltas[entity_name] = other_delta.deltas[entity_name]

        return new_delta

    def copy(self):

        aspects_vals_per_entity = {}
        for entity_name, delta in self.deltas.items():
            aspects_vals_per_entity[entity_name] = delta.to_raw_change()

        return self.__class__.from_entity_group_deltas(self.deltas.copy(),
                                                       self.in_change,
                                                       self.virtual)

    def add(self,
            other_delta : EntityGroupDelta):

        if not isinstance(other_delta, EntityGroupDelta):
            raise TypeError(f'An attempt to add an object of unknown type {other_delta.__class__.__name__} to the list of deltas in {self.__class__.__name__}')

        self.deltas[other_delta.entity_name] = other_delta

    def extract_raw_scaling_aspects_changes(self):

        raw_representation = {}
        for entity_name, entity_delta in self.deltas.items():
            raw_representation[entity_name] = entity_delta.to_raw_change()

        return raw_representation


    def enforce(self,
                entities_lst : list):

        """
        Enforces the entity group delta change for entities provided in the list.
        Results in splitting the delta into two. The first one which is enforced,
        and the second one that contains the unenforced remainder to consider
        further (entities that have later enforcement time).
        """

        enforced_deltas = {}
        non_enforced_deltas = self.deltas.copy()

        for entity_name in entities_lst:
            if entity_name in non_enforced_deltas:
                enforced_deltas[entity_name] = non_enforced_deltas[entity_name].copy()
                del non_enforced_deltas[entity_name]

        enforced_egd = EntitiesGroupDelta.from_entity_group_deltas(enforced_deltas,
                                                                   False)
        non_enforced_egd = EntitiesGroupDelta.from_entity_group_deltas(non_enforced_deltas,
                                                                       True)

        return (enforced_egd, non_enforced_egd)
