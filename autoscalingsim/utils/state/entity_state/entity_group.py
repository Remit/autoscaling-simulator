import numbers
import collections
import numpy as np

from . import scaling_aspects
from .scaling_aspects import ScalingAspect, ScalingAspectDelta

from ...error_check import ErrorChecker

class EntityGroup:

    """
    Wraps the state of multiple scaled entities. The state is divided into
    the static and dynamic parts. The static part is the current count of
    entities. The dynamic part is the count of booting/terminating entities.
    """

    def __init__(self,
                 entity_name : str,
                 aspects_vals = {'count': 1}):

        """
        If just a single integer is provided as a param aspects_vals, then it is
        considered to be the count of entities.
        """

        self.entity_name = entity_name
        self.scaling_aspects = {}

        if isinstance(aspects_vals, collections.Mapping):
            for aspect_name, aspect_value in aspects_vals.items():
                if isinstance(aspect_value, ScalingAspect):
                    self.scaling_aspects[aspect_name] = aspect_value
                elif isinstance(aspect_value, numbers.Number):
                    self.scaling_aspects[aspect_name] = scaling_aspects.Registry.get(aspect_name)(aspect_value)
                else:
                    raise TypeError('Unexpected type of scaling aspects values to initialize the {}'.format(self.__class__))
        else:
            raise TypeError('Unexpected type of scaling aspects dictionary to initialize the {}'.format(self.__class__))

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
            raise TypeError('Incorrect type of operand to _add to {}: {}'.format(self.__class__,
                                                                                 type(other_group_or_delta)))

        if self.entity_name != other_group_or_delta.entity_name:
            raise ValueError('Non-matching names of operands to _add: {} and {}'.format(self.entity_name,
                                                                                        other_group_or_delta.entity_name))

        new_group = self.copy()
        for aspect_name in to_add:
            if aspect_name in new_group.scaling_aspects:
                if sign == -1:
                    new_group.scaling_aspects[aspect_name] -= to_add[aspect_name]
                elif sign == 1:
                    new_group.scaling_aspects[aspect_name] += to_add[aspect_name]
            elif sign == 1:
                new_group.scaling_aspects[aspect_name] = scaling_aspects.Registry.get(aspect_name)()
                new_group.scaling_aspects[aspect_name] += to_add[aspect_name]

        return new_group

    def __mul__(self,
                multiplier : int):

        if not isinstance(multiplier, int):
            raise TypeError('Incorrect type of mulitiplier to multiply {} by: {}'.format(self.__class__, multiplier.__class__))

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
            raise TypeError('An attempt to floor-divide by an unknown type {}'.format(type(other_entity_group)))

        division_results = []
        for aspect_name, aspect_value in self.scaling_aspects.items():
            if aspect_name in other_entity_group.scaling_aspects:
                division_results.append(aspect_value // other_entity_group.scaling_aspects[aspect_name])

        return division_results

    def __mod__(self,
                other_entity_group):

        if not isinstance(other_entity_group, self.__class__):
            raise TypeError('Incorrect type of operand to take modulo of {}: {}'.format(self.__class__,
                                                                                        type(other_entity_group)))

        if self.entity_name != other_entity_group.entity_name:
            raise ValueError('Non-matching names of EntityGroups to take modulo: {} and {}'.format(self.entity_name,
                                                                                                   other_entity_group.entity_name))

        new_aspects = self.scaling_aspects.copy()
        for aspect_name, aspect in self.scaling_aspects.items():
            if aspect_name in other_entity_group.scaling_aspects:
                new_aspects[aspect_name] %= other_entity_group.scaling_aspects[aspect_name]

        return EntityGroup(self.entity_name, new_aspects)

    def copy(self):

        return EntityGroup(self.entity_name,
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
            raise ValueError('Unexpected aspect for an update: {}'.format(aspect.name))

        self.scaling_aspects[aspect.name] = aspect

    def get_aspect_value(self,
                         aspect_name : str):

        if not aspect_name in self.scaling_aspects:
            raise ValueError('Unexpected aspect for an update: {}'.format(aspect_name))

        return self.scaling_aspects[aspect_name].copy()

class EntityGroupDelta:

    """
    Wraps the entity group change and the direction of change, i.e. addition
    or subtraction.
    """

    @staticmethod
    def from_group(entity_group : EntityGroup,
                   sign = 1):

        if not isinstance(sign, int):
            raise TypeError('The provided sign parameters is not of {} type'.format(int.__name__))

        if not isinstance(entity_group, EntityGroup):
            raise TypeError('The provided argument is not of EntityGroup type: {}'.format(entity_group.__class__))

        aspects_vals_raw_numbers = {}
        for aspect_name, aspect_value in entity_group.scaling_aspects.items():
            aspects_vals_raw_numbers[aspect_name] = sign * aspect_value.get_value()

        return EntityGroupDelta(entity_group.entity_name,
                                aspects_vals_raw_numbers)

    def __init__(self,
                 entity_name : str,
                 aspects_vals : dict):

        self.entity_name = entity_name
        self.aspects_deltas = {}
        for aspect_name, aspect_value in aspects_vals.items():
            if isinstance(aspect_value, ScalingAspect):
                self.aspects_deltas[aspect_name] = ScalingAspectDelta(aspect_value)
            elif isinstance(aspect_value, numbers.Number):
                self.aspects_deltas[aspect_name] = ScalingAspectDelta(scaling_aspects.Registry.get(aspect_name)(abs(aspect_value)),
                                                                                      int(np.sign(aspect_value)))
            else:
                raise TypeError('Unexpected type of scaling aspects values to initialize the {}'.format(self.__class__))

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
            raise TypeError('The operand to be added is not of the expected type {}: instead got {}'.format(self.__class__,
                                                                                                            type(other_delta)))

        if self.entity_name != other_delta.entity_name:
            raise ValueError('An attempt to add {} with different names: {} and {}'.format(self.__class__,
                                                                                           self.entity_name,
                                                                                           other_delta.entity_name))

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
                                self.to_raw_change())

    def to_raw_change(self):

        aspects_vals_raw_numbers = {}
        for aspect_name, aspect_delta in self.aspects_deltas.items():
            aspects_vals_raw_numbers[aspect_name] = aspect_delta.to_raw_change()

        return aspects_vals_raw_numbers

    def to_entity_group(self):

        return EntityGroup(self.entity_name,
                           self.to_raw_change())

    def get_aspect_change_sign(self,
                               scaled_aspect_name : str):

        if scaled_aspect_name in self.aspects_deltas:
            return self.aspects_deltas[scaled_aspect_name].sign
        else:
            raise ValueError('Aspect {} not found in {}'.format(scaled_aspect_name,
                                                                self.__class__))


class EntitiesGroupDelta:

    """
    Wraps multiple EntityGroupDelta distinguished by the entity.
    """

    @staticmethod
    def from_entity_group_deltas(deltas : dict,
                                 in_change = True):

        entities_group_delta = EntitiesGroupDelta({}, in_change)
        entities_group_delta.deltas = deltas

        return entities_group_delta

    def __init__(self,
                 aspects_vals_per_entity : dict = {},
                 in_change : bool = True,
                 virtual : bool = False):

        self.deltas = {}
        for entity_name, aspects_vals in aspects_vals_per_entity.items():

            self.deltas[entity_name] = EntityGroupDelta(entity_name,
                                                        aspects_vals)

        # Signifies whether the delta should be considered during the enforcing or not.
        # The aim of 'virtual' property is to keep the connection between the deltas after the enforcement.
        self.virtual = virtual
        self.in_change = in_change

    def get_entities(self):

        return list(self.deltas.keys())

    def get_entity_group_delta(self,
                               entity_name : str):

        if not entity_name in self.deltas:
            raise ValueError('No entity group delta for entity name {} found'.format(entity_name))

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
            raise TypeError('The operand to be added is not of the expected type {}: instead got {}'.format(self.__class__,
                                                                                                            type(other_delta)))

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
            aspects_vals_per_entity[aspect_name] = delta.to_raw_change()

        return EntitiesGroupDelta(aspects_vals_per_entity,
                                  self.in_change,
                                  self.virtual)

    def add(self,
            other_delta : EntityGroupDelta):

        if not isinstance(other_delta, EntityGroupDelta):
            raise TypeError('An attempt to add an object of unknown type {} to the list of deltas in {}'.format(type(other_delta),
                                                                                                                self.__class__))

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
