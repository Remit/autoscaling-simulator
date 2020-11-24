import numbers
import collections
import numpy as np
from abc import ABC

from .scaling_aspects import ScalingAspect, ScalingAspectDelta

from ...utils.error_check import ErrorChecker
from ...utils.requirements import ResourceRequirements

class ServiceInstancesGroup:

    """ A primitive that represents a group of instances of the same service  """

    def __init__(self, service_name : str, service_resource_reqs : ResourceRequirements,
                 aspects_vals = {'count': 1}):

        self.service_name = service_name
        self.scaling_aspects = {}

        if not isinstance(service_resource_reqs, ResourceRequirements):
            raise TypeError(f'Unexpected type for the service resource requirements: {service_resource_reqs.__class__.__name__}')
        self.service_resource_reqs = service_resource_reqs

        if isinstance(aspects_vals, collections.Mapping):
            for aspect_name, aspect_value in aspects_vals.items():
                if isinstance(aspect_value, ScalingAspect):
                    self.scaling_aspects[aspect_name] = aspect_value
                elif isinstance(aspect_value, numbers.Number):
                    self.scaling_aspects[aspect_name] = ScalingAspect.get(aspect_name)(aspect_value)
                else:
                    raise TypeError(f'Unexpected type of scaling aspects values to initialize {self.__class__.__name__}')
        else:
            raise TypeError(f'Unexpected type of scaling aspects dictionary to initialize {self.__class__.__name__}')

    def downsize_proportionally(self, downsizing_coef : float):

        self.scaling_aspects['count'] *= (1 - downsizing_coef)

    def __add__(self, other):

        return self._add(other, 1)

    def __sub__(self, other):

        return self._add(other, -1)

    def _add(self, other_group_or_delta, sign : int):

        to_add = None
        if isinstance(other_group_or_delta, ServiceInstancesGroup):
            to_add = other_group_or_delta.scaling_aspects
        elif isinstance(other_group_or_delta, ServiceInstancesGroupDeltaCommon):
            to_add = other_group_or_delta.aspects_deltas
        else:
            raise TypeError(f'Incorrect type of operand to combine with {self.__class__.__name__}: {other_group_or_delta.__class__.__name__}')

        if self.service_name != other_group_or_delta.service_name:
            raise ValueError(f'Non-matching names of services to combine: {self.service_name} and {other_group_or_delta.service_name}')

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

    def __mul__(self, multiplier : int):

        if not isinstance(multiplier, int):
            raise TypeError(f'Incorrect type of mulitiplier for {self.__class__.__name__}: {multiplier.__class__.__name__}')

        new_aspects = self.scaling_aspects.copy()
        for aspect_name, aspect in self.scaling_aspects.items():
            new_aspects[aspect_name] *= multiplier

        return self.__class__(self.service_name, new_aspects)

    def __floordiv__(self, other : 'ServiceInstancesGroup'):

        """
        Returns the list of values. Each value corresponds to a scaling aspect and
        signifies how many times does the scaling aspect of the current group covers
        the corresponding scaling aspect of the parameter.
        """

        if not isinstance(other, ServiceInstancesGroup):
            raise TypeError(f'An attempt to floor-divide by an unknown type {other.__class__.__name__}')

        division_results = []
        for aspect_name, aspect_value in self.scaling_aspects.items():
            if aspect_name in other.scaling_aspects:
                division_results.append((aspect_value // other.scaling_aspects[aspect_name]).get_value())

        return division_results

    def __mod__(self, other):

        if not isinstance(other, self.__class__):
            raise TypeError(f'Incorrect type of operand to take modulo of {self.__class__.__name__}: {other.__class__.__name__}')

        if self.service_name != other.service_name:
            raise ValueError(f'Non-matching names of services to take modulo: {self.service_name} and {other.service_name}')

        new_aspects = self.scaling_aspects.copy()
        for aspect_name, aspect in self.scaling_aspects.items():
            if aspect_name in other.scaling_aspects:
                new_aspects[aspect_name] %= other.scaling_aspects[aspect_name]

        return self.__class__(self.service_name, new_aspects)

    def is_empty(self):

        return (self.scaling_aspects['count'] == ScalingAspect.get('count')(0))

    def update_aspect(self, aspect : ScalingAspect):

        if not aspect.name in self.scaling_aspects:
            raise ValueError(f'Unexpected aspect for an update: {aspect.name}')

        self.scaling_aspects[aspect.name] = aspect

    def get_aspect_value(self, aspect_name : str):

        if not aspect_name in self.scaling_aspects:
            raise ValueError(f'Unexpected aspect to get: {aspect_name}')

        return self.scaling_aspects[aspect_name].copy()

    def get_resource_requirements(self):

        return self.service_resource_reqs

    def copy(self):

        return self.__class__(self.service_name, self.service_resource_reqs,
                              self.scaling_aspects.copy())

    def to_delta(self, direction = 1):

        return ServiceInstancesGroupDelta.from_group(self, direction)

class ServiceInstancesGroupDeltaCommon(ABC):

    def __init__(self, service_name : str, aspects_vals : dict):

        self.service_name = service_name
        self.aspects_deltas = {}
        for aspect_name, aspect_value in aspects_vals.items():
            if isinstance(aspect_value, ScalingAspectDelta):
                self.aspects_deltas[aspect_name] = aspect_value
            elif isinstance(aspect_value, ScalingAspect):
                self.aspects_deltas[aspect_name] = ScalingAspectDelta(aspect_value)
            elif isinstance(aspect_value, numbers.Number):
                self.aspects_deltas[aspect_name] = ScalingAspectDelta(ScalingAspect.get(aspect_name)(abs(aspect_value)),
                                                                      int(np.sign(aspect_value)))
            else:
                raise TypeError(f'Unexpected type of scaling aspects values to initialize {self.__class__.__name__}')

    def set_count_sign(self, sign : int):

        if 'count' in self.aspects_deltas: self.aspects_deltas['count'].sign = sign

    def to_raw_change(self):

        return { aspect_name : aspect_delta.to_raw_change() for aspect_name, aspect_delta in self.aspects_deltas.items() }

    def __add__(self, other_delta : 'ServiceInstancesGroupDelta'):

        return self._add(other_delta, 1)

    def __sub__(self, other_delta : 'ServiceInstancesGroupDelta'):

        return self._add(other_delta, -1)

    def _add(self, other_delta : 'ServiceInstancesGroupDelta', sign : int):

        if not isinstance(other_delta, ServiceInstancesGroupDelta):
            raise TypeError(f'The operand to be combined is not of the expected type {self.__class__.__name__}, got {other_delta.__class__.__name__}')

        if self.service_name != other_delta.service_name:
            raise ValueError(f'An attempt to combine services with different names: {self.service_name} and {other_delta.service_name}')

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

    def get_aspect_change_sign(self, aspect_name : str):

        if not aspect_name in self.aspects_deltas:
            raise ValueError(f'Aspect {aspect_name} not found in {self.__class__.__name__}')

        return self.aspects_deltas[aspect_name].sign

class ServiceInstancesGroupDeltaWildcard(ServiceInstancesGroupDeltaCommon):

    """
    A wildcard service instances group delta represents a change to be applied
    to any existing group of the service that it is associated with. Such a
    delta can represent a service instance failure.
    """

    def __init__(self, service_name : str, aspects_vals : dict):

        super().__init__(service_name, aspects_vals)

    def __repr__(self):

        return f'{self.__class__.__name__}({self.service_name}, {repr(self.aspects_deltas)})'

class ServiceInstancesGroupDelta(ServiceInstancesGroupDeltaCommon):

    """ Represents a concrete change for the associated service """

    @classmethod
    def from_group(cls : type, service_group : ServiceInstancesGroup, sign : int = 1):

        if not isinstance(sign, int):
            raise TypeError(f'The provided sign parameters is not of {int.__name__} type: {sign.__class__.__name__}')

        if not isinstance(service_group, ServiceInstancesGroup):
            raise TypeError(f'The provided argument is not of ServiceInstancesGroup type: {service_group.__class__.__name__}')

        return cls(service_group.service_name,
                   { aspect_name : sign * aspect_value.get_value() for aspect_name, aspect_value in service_group.scaling_aspects.items() },
                   service_group.service_resource_reqs)

    def __init__(self, service_name : str, aspects_vals : dict, service_resource_reqs : ResourceRequirements):

        super().__init__(service_name, aspects_vals)

        if not isinstance(service_resource_reqs, ResourceRequirements):
            raise TypeError(f'Unexpected type for entity resource requirements when initializing {self.__class__.__name__}: {service_resource_reqs.__class__.__name__}')

        self.service_resource_reqs = service_resource_reqs

    def to_service_group(self):

        return ServiceInstancesGroup(self.service_name, self.service_resource_reqs,
                                     self.to_raw_change())

    def copy(self):

        return self.__class__(self.service_name, self.to_raw_change(), self.service_resource_reqs)

    def __repr__(self):

        return f'{self.__class__.__name__}({self.service_name}, {repr(self.aspects_deltas)}, {repr(self.service_resource_reqs)})'

class GroupOfServices:

    """ Combines service instances groups for multiple services """

    def __init__(self, groups_or_aspects : dict = {}, services_resource_reqs : dict = {}):

        self.services_groups = {}

        for service_name, group_or_aspects_dict in groups_or_aspects.items():
            if isinstance(group_or_aspects_dict, ServiceInstancesGroup):
                self.services_groups[service_name] = group_or_aspects_dict
            elif isinstance(groups_or_aspects, collections.Mapping):
                if len(services_resource_reqs) == 0:
                    raise ValueError(f'No resource requirements provided for the initialization of {self.__class__.__name__}')

                self.services_groups[service_name] = ServiceInstancesGroup(service_name, services_resource_reqs[service_name],
                                                                           group_or_aspects_dict)
            else:
                raise TypeError(f'Unknown type of the init parameter: {groups_or_aspects.__class__.__name__}')

    def can_be_coerced(self, services_group_delta : 'GroupOfServicesDelta') -> bool:

        if not isinstance(services_group_delta, GroupOfServicesDelta):
            raise TypeError(f'Unexpected type for coercion: {services_group_delta.__class__.__name__}')

        for service_name, change_val in services_group_delta.to_services_raw_count_change().items():
            if not service_name in self.services_groups: return False

        return True

    def downsize_proportionally(self, downsizing_coef : float):

        for service_group in self.services_groups.values():
            service_group.downsize_proportionally(downsizing_coef)

    def get_services_counts(self) -> dict:

        return { service_name : group.get_aspect_value('count').get_value() for service_name, group in self.services_groups.items() }

    def get_services(self) -> list:

        return list(self.services_groups.keys())

    def get_services_requirements(self) -> dict:

        return { service_name : group.get_resource_requirements() for service_name, group in self.services_groups.items() }

    def copy(self):

        return self.__class__(self.services_groups.copy())

    def __add__(self, other):

        return self._add(other, 1)

    def __sub__(self, other):

        return self._add(other, -1)

    def _add(self, other, sign : int):

        new_groups = {}
        if isinstance(other, GroupOfServicesDelta):
            if other.in_change:
                raise ValueError('Cannot add the delta that is still in change to the current entities state')
            else:
                for service_name, service_delta in other.deltas.items():
                    if service_name in self.services_groups:
                        if sign == -1:
                            new_groups[service_name] = self.services_groups[service_name] - service_delta
                        elif sign == 1:
                            new_groups[service_name] = self.services_groups[service_name] + service_delta
                    elif sign == 1:
                        new_groups[service_name] = service_delta.to_service_group()

        elif isinstance(other, self.__class__):
            for service_name, service_group_to_add in other.services_groups.items():
                if service_name in self.services_groups:
                    if sign == -1:
                        new_groups[service_name] = self.services_groups[service_name] - service_group_to_add
                    elif sign == 1:
                        new_groups[service_name] = self.services_groups[service_name] + service_group_to_add
                elif sign == 1:
                    new_groups[service_name] = service_group_to_add
        else:
            raise TypeError(f'An attempt to add the operand of type {services_to_add.__class__.__name__} to the {self.__class__.__name__} when expecting type GroupOfServicesDelta or GroupOfServices')

        # Prune new groups from 0-sized groups
        #new_groups_adj = {}
        #for service_name, group in new_groups.items():
        #    if not group.is_empty():
        #        new_groups_adj[service_name] = group

        return self.__class__(new_groups)

    def __truediv__(self, other : 'GroupOfServices'):

        """
        Computes the count of *full* argument groups that the current group
        of services is composed of. The remainder is calculated with __mod__.
        """

        if not isinstance(other, self.__class__):
            raise TypeError(f'Incorrect type of operand to divide {self.__class__.__name__} by: {other.__class__.__name__}')

        return min([min(service_group // other.services_groups[service_name]) if service_name in other.services_groups else 0 \
                    for service_name, service_group in self.services_groups.items()])

    def __mod__(self, other : 'GroupOfServices'):

        """
        Computes the remainder group of services that should be merged with
        some number of argument groups to get the current group of services.
        """

        if not isinstance(other, self.__class__):
            raise TypeError(f'Incorrect type of operand to take {self.__class__.__name__} modulo: {other.__class__.__name__}')

        return self.__class__({ service_name : service_group % other.services_groups[service_name] if service_group in other.services_groups else service_group \
                                for service_name, service_group in self.services_groups.items()})

    def to_delta(self, direction : int = 1):

        return GroupOfServicesDelta.from_deltas({ service_name : group.to_delta(direction) for service_name, group in self.services_groups.items() })

    def get_scaling_aspects_for_every_service(self):

        return { service_name : service_group.scaling_aspects for service_name, service_group in self.services_groups.items() }

    # was extract_aspect_representation
    def get_scaling_aspect_for_every_service(self, aspect_name : str):

        return { service_name : service_group.get_aspect_value(aspect_name) for service_name, service_group in self.services_groups.items() }

    # was extract_aspect_value
    def get_raw_aspect_value_for_every_service(self, aspect_name : str):

        return { service_name : service_group.get_aspect_value(aspect_name).get_value() for service_name, service_group in self.services_groups.items() }

    # was get_aspect_value
    def get_aspect_value_for_service(self, service_name : str, aspect_name : str):

        return self.services_groups[service_name].get_aspect_value(aspect_name) if service_name in self.services_groups else ScalingAspect.get(aspect_name)(0)

    def get_service_count(self, service_name : str):

        return self.get_aspect_value_for_service(service_name, 'count').get_value()

    def get_service_resource_requirements(self, service_name : str):

        if not service_name in self.services_groups:
            raise ValueError(f'An attempt to get the resource requirements for an unknown service: {service_name}')

        return self.services_groups[service_name].get_resource_requirements()


class GroupOfServicesDelta:

    @classmethod
    def from_deltas(cls : type, deltas : dict, in_change : bool = True, virtual : bool = False):

        services_group_delta = cls({}, in_change, virtual)
        services_group_delta.deltas = deltas

        return services_group_delta

    def __init__(self, aspects_vals_per_entity : dict = {},
                 in_change : bool = True, virtual : bool = False, services_reqs : dict = {}):

        self.deltas = { service_name : ServiceInstancesGroupDelta(service_name, aspects_vals, services_reqs[service_name]) \
                                        if service_name in services_reqs else ServiceInstancesGroupDeltaWildcard(service_name, aspects_vals) \
                                        for service_name, aspects_vals in aspects_vals_per_entity.items() }

        # Signifies whether the delta should be considered during the enforcing or not.
        # The aim of 'virtual' property is to keep the connection between the deltas after the enforcement.
        self.virtual = virtual
        self.in_change = in_change

    def enforce(self, services_lst : list):

        """
        Enforces the change represented by this delta for entities provided in the list.
        Results two deltas. The first is enforced, and the second contains the unenforced
        remainder to consider later on (e.g. later enforcement time).
        """

        enforced_deltas = { service_name : delta for service_name, delta in self.deltas.items() if service_name in services_lst }
        not_enforced_deltas = { service_name : delta for service_name, delta in self.deltas.items() if not service_name in services_lst }

        return (self.__class__.from_deltas(enforced_deltas, in_change = False),
                self.__class__.from_deltas(not_enforced_deltas, in_change = True))

    def __add__(self, other):

        return self._add(other, 1)

    def __sub__(self, other):

        return self._add(other, -1)

    def _add(self, other_delta : 'GroupOfServicesDelta', sign : int):

        if not isinstance(other_delta, GroupOfServicesDelta):
            raise TypeError(f'The operand to be added is not of the expected type {self.__class__.__name__}, got {other_delta.__class__.__name__}')

        if self.in_change != other_delta.in_change:
            raise ValueError('Operands differ by the in_change status')

        new_deltas = self.deltas.copy()
        for service_name in other_delta.deltas:
            if service_name in new_deltas:
                if sign == -1:
                    new_deltas[service_name] -= other_delta.deltas[service_name]
                elif sign == 1:
                    new_deltas[service_name] += other_delta.deltas[service_name]
            else:
                new_deltas[service_name] = other_delta.deltas[service_name]

        return self.__class__.from_deltas({ service_name : delta for service_name, delta in new_deltas.items() if delta.to_raw_change()['count'] != 0 },
                                          self.in_change, self.virtual)

    def add(self, other_delta : ServiceInstancesGroupDelta):

        if not isinstance(other_delta, ServiceInstancesGroupDelta):
            raise TypeError(f'An attempt to add an object of unknown type {other_delta.__class__.__name__} to the list of deltas in {self.__class__.__name__}')

        self.deltas[other_delta.service_name] = other_delta

    def set_count_sign(self, sign : int):

        for delta in self.deltas.values():
            delta.set_count_sign(sign)

    def get_services(self):

        return list(self.deltas.keys())

    # was get_service_group_delta
    def get_delta_for_service(self, service_name : str):

        if not service_name in self.deltas:
            raise ValueError(f'No entity group delta for entity name {service_name} found')

        return self.deltas[service_name]

    # was extract_raw_scaling_aspects_changes
    def get_raw_scaling_aspects_changes(self):

        return { service_name : service_delta.to_raw_change() for service_name, service_delta in self.deltas.items() }

    def to_services_raw_count_change(self):

        return { service_name : delta.to_raw_change()['count'] for service_name, delta in self.deltas.items() }

    def to_services_state(self):

        return GroupOfServices({ service_name : delta.to_service_group() for service_name, delta in self.deltas.items() })

    def copy(self):

        return self.__class__.from_deltas(self.deltas.copy(), self.in_change, self.virtual)

    def __repr__(self):

        return f'{self.__class__.__name__}( deltas = {repr(self.deltas)}, in_change = {self.in_change}, virtual = {self.virtual})'
