import numbers
import math

from abc import ABC, abstractmethod
from autoscalingsim.scaling.scaling_aspects import ScalingAspect
from autoscalingsim.deltarepr.scaling_aspect_delta import ScalingAspectDelta
from autoscalingsim.utils.requirements import ResourceRequirements

class ServiceInstancesGroupDeltaCommon(ABC):

    @abstractmethod
    def to_concrete_delta(self, service_resource_reqs : ResourceRequirements):

        pass

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
                                                                      int(math.copysign(1, aspect_value)))
            else:
                raise TypeError(f'Unexpected type of scaling aspects values to initialize {self.__class__.__name__}')

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

    def set_count_sign(self, sign : int):

        if 'count' in self.aspects_deltas:
            self.aspects_deltas['count'].sign = sign

    def to_raw_change(self):

        return { aspect_name : aspect_delta.to_raw_change() for aspect_name, aspect_delta in self.aspects_deltas.items() }

    def sign_for_aspect(self, aspect_name : str):

        if not aspect_name in self.aspects_deltas:
            raise ValueError(f'Aspect {aspect_name} not found in {self.__class__.__name__}')

        return self.aspects_deltas[aspect_name].sign

    @property
    def is_empty(self):

        for aspect_name, aspect_delta in self.aspects_deltas.items():
            if aspect_delta.to_raw_change() != 0:
                return False

        return True

class ServiceInstancesGroupDeltaWildcard(ServiceInstancesGroupDeltaCommon):

    """
    This wildcard delta represents a change to be applied to *any* existing
    service instances group of a given service. Represents a service instance(s) failure.
    """

    def __init__(self, service_name : str, aspects_vals : dict):

        super().__init__(service_name, aspects_vals)

    def to_concrete_delta(self, service_resource_reqs : ResourceRequirements):

        return ServiceInstancesGroupDelta(self.service_name, self.to_raw_change(), service_resource_reqs)

    def __repr__(self):

        return f'{self.__class__.__name__}({self.service_name}, {repr(self.aspects_deltas)})'

class ServiceInstancesGroupDelta(ServiceInstancesGroupDeltaCommon):

    """ Represents the change for an associated service """

    @classmethod
    def from_group(cls, service_group : 'ServiceInstancesGroup', sign : int = 1):

        return cls(service_group.service_name,
                   { aspect_name : sign * aspect_value.value for aspect_name, aspect_value in service_group.scaling_aspects.items() },
                   service_group.service_resource_reqs)

    def __init__(self, service_name : str, aspects_vals : dict, service_resource_reqs : ResourceRequirements):

        super().__init__(service_name, aspects_vals)

        self.service_resource_reqs = service_resource_reqs

    def to_concrete_delta(self, service_resource_reqs : ResourceRequirements):

        return self.copy()

    def to_service_group(self):

        import autoscalingsim.desired_state.service_group.service_instances_group as sig

        return sig.ServiceInstancesGroup(self.service_name, self.service_resource_reqs, self.to_raw_change())

    def copy(self):

        return self.__class__(self.service_name, self.to_raw_change(), self.service_resource_reqs)

    def __repr__(self):

        return f'{self.__class__.__name__}({self.service_name}, {repr(self.aspects_deltas)}, {repr(self.service_resource_reqs)})'
