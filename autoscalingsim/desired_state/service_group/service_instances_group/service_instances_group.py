import numbers
import collections
import numpy as np

from autoscalingsim.deltarepr.service_instances_group_delta import ServiceInstancesGroupDeltaCommon, ServiceInstancesGroupDelta
from autoscalingsim.scaling.scaling_aspects import ScalingAspect
from autoscalingsim.utils.requirements import ResourceRequirements

class ServiceInstancesGroup:

    def __init__(self, service_name : str, service_resource_reqs : ResourceRequirements,
                 aspects_vals : dict = None):

        self.service_name = service_name
        self.service_resource_reqs = service_resource_reqs
        self.scaling_aspects = collections.defaultdict(ScalingAspect)

        aspects_vals_raw = { 'count' : 1 } if aspects_vals is None else aspects_vals

        for aspect_name, aspect_value in aspects_vals_raw.items():
            if isinstance(aspect_value, ScalingAspect):
                self.scaling_aspects[aspect_name] = aspect_value

            elif isinstance(aspect_value, numbers.Number):
                self.scaling_aspects[aspect_name] = ScalingAspect.get(aspect_name)(aspect_value)

    def __add__(self, other):

        return self._add(other, 1)

    def __sub__(self, other):

        return self._add(other, -1)

    def _add(self, other, sign : int):

        to_add = None

        if isinstance(other, ServiceInstancesGroup):
            to_add = other.scaling_aspects

        elif isinstance(other, ServiceInstancesGroupDeltaCommon):
            to_add = other.aspects_deltas

        new_group = self.copy()
        for aspect_name in to_add:
            if aspect_name in new_group.scaling_aspects:
                if sign == -1:
                    new_group.scaling_aspects[aspect_name] -= to_add[aspect_name]
                elif sign == 1:
                    new_group.scaling_aspects[aspect_name] += to_add[aspect_name]
            elif sign == 1:
                new_group.scaling_aspects[aspect_name] = ScalingAspect.get(aspect_name)() + to_add[aspect_name]

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
                division_results.append((aspect_value // other.scaling_aspects[aspect_name]).value)

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

    def downsize_proportionally(self, downsizing_coef : float):

        self.scaling_aspects['count'] *= (1 - downsizing_coef)

    def update_aspect(self, aspect : ScalingAspect):

        self.scaling_aspects[aspect.name] = aspect

    def aspect_value(self, aspect_name : str):

        return self.scaling_aspects[aspect_name].copy()

    def to_delta(self, direction = 1):

        return ServiceInstancesGroupDelta.from_group(self, direction)

    @property
    def is_empty(self):

        return self.scaling_aspects['count'] == ScalingAspect.get('count')(0)

    @property
    def resource_requirements(self):

        return self.service_resource_reqs.copy()

    def copy(self):

        return self.__class__(self.service_name, self.service_resource_reqs, self.scaling_aspects.copy())

    def __repr__(self):

        return f'{self.__class__.__name__}(service_name = {self.service_name}, \
                                           service_resource_reqs = {self.service_resource_reqs}, \
                                           aspects_vals = {self.scaling_aspects})'
