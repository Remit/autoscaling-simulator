import operator
import numbers
import math
from copy import deepcopy

from autoscalingsim.utils.metric.metric_categories.size import Size
from autoscalingsim.utils.metric.metric_categories.numeric import Numeric

class SystemResourceUsage:

    """
    Represents system resources usage, e.g. vCPU, memory, and network bandwidth.
    Since it is bound to the system resources available on a particular node type,
    each arithmetic operation on the system resource usage checks whether the
    operands have the matching node types.
    """

    MAX_ALLOWED_SYSTEM_RESOURCE_USAGE_BY_SERVICES = 0.35
    MAX_ALLOWED_VCPU_SHARING_FACTOR = 2

    system_resources = {
        'vCPU'              : Numeric,
        'memory'            : Size,
        'disk'              : Size,
        'network_bandwidth' : Size
    }

    def __init__(self,
                 node_info : 'NodeInfo',
                 instance_count : int = 1,
                 system_resources_usage : dict = {'vCPU' : Numeric(0), 'memory' : Size(0),
                                                  'disk': Size(0),
                                                  'network_bandwidth' : Size(0)}):

        self.node_info = node_info

        self.uid = node_info.unique_id
        self.instance_max_usage = node_info.max_usage
        self.instance_count = instance_count

        self.system_resources_usage = dict()
        for res_name, cls in self.__class__.system_resources.items():
            self.system_resources_usage[res_name] = system_resources_usage[res_name] if res_name in system_resources_usage else cls(0)

    def __add__(self, usage_to_add : 'SystemResourceUsage'):

        return self._add(usage_to_add, 1)

    def __sub__(self, usage_to_sub : 'SystemResourceUsage'):

        return self._add(usage_to_sub, -1)

    def _add(self, other_usage : 'SystemResourceUsage', sign : int = 1):

        if not isinstance(other_usage, self.__class__):
            raise ValueError(f'An attempt to combine an object of type {other_usage.__class__.__name__} with an object of type {self.__class__.__name__}')

        if self.uid != other_usage.uid:
            raise ValueError(f'An attempt to combine resource usages belonging to different node types: {self.uid} and {other_usage.uid}')

        if not (other_usage.instance_count == 1 or self.instance_count == 1) and self.instance_count != other_usage.instance_count:
            raise ValueError(f'Attempt to combine system resource usages for unmatching cluster sizes')

        system_resource_usage = deepcopy(self.system_resources_usage)
        for res_name, res_usage in other_usage.system_resources_usage.items():
            if res_name in system_resource_usage:
                system_resource_usage[res_name] += sign * res_usage

        return self.__class__(self.node_info, self.instance_count, system_resource_usage)

    def __mul__(self, multiplier : float):

        if not isinstance(multiplier, numbers.Number):
            raise ValueError(f'An attempt to mutiply the {self.__class__.__name__} by non-number')

        system_resource_usage = {res_name : res_usage * multiplier for res_name, res_usage in self.system_resources_usage.items()}

        return self.__class__(self.node_info, self.instance_count, system_resource_usage)

    def __floordiv__(self, other_usage : 'SystemResourceUsage'):

        """ Determines how many other usage can fit into the current usage """

        return max([math.ceil(res_usage / other_res_usage) \
                        for other_res_name, other_res_usage in other_usage.system_resources_usage.items() \
                        for res_name, res_usage in self.system_resources_usage.items() \
                        if other_res_name == res_name])

    def compress(self):

        new_instances_count_by_resource = dict()
        for res_name, res_usage in self.system_resources_usage.items():
            new_instances_count_by_resource[res_name] = math.ceil(res_usage / (self.instance_max_usage[res_name] * self.__class__.MAX_ALLOWED_SYSTEM_RESOURCE_USAGE_BY_SERVICES))

        new_instances_count = max(new_instances_count_by_resource.values()) if len(new_instances_count_by_resource) > 0 else self.instance_count

        return self.__class__(self.node_info, min(new_instances_count, self.instance_count), deepcopy(self.system_resources_usage))

    @property
    def max_threads(self):

        return self.instance_count * self.__class__.MAX_ALLOWED_VCPU_SHARING_FACTOR * self.node_info.vCPU_count

    @property
    def can_accommodate_another_service_instance(self):

        for res_name, res_usage in self.system_resources_usage.items():
            if res_usage >= self.__class__.MAX_ALLOWED_SYSTEM_RESOURCE_USAGE_BY_SERVICES * self.instance_count * self.instance_max_usage[res_name]:
                return False

        return True

    @property
    def is_full(self):

        """ Checks whether the system resources are exhausted """

        for res_name, res_usage in self.system_resources_usage.items():
            if res_usage >= self.instance_count * self.instance_max_usage[res_name]:
                return True

        return False

    @property
    def is_zero(self):

        """ Checks whether at least something takes system resources """

        for res_name, res_usage in self.system_resources_usage.items():
            if res_usage > self.__class__.system_resources[res_name](0):
                return False

        return True

    def to_dict(self):

        return self.system_resources_usage.copy()

    def cap(self):

        system_resources_usage = { res_name : min(res_usage, self.instance_count * self.instance_max_usage[res_name]) for res_name, res_usage in self.system_resources_usage.items() }

        return self.__class__(self.node_info, self.instance_count, system_resources_usage)


    def normalized_usage(self, res_name : str):

        if res_name in self.system_resources_usage:
            return 0 if self.instance_count == 0 else self.system_resources_usage[res_name] / (self.instance_count * self.instance_max_usage[res_name])
        else:
            return 0

    def as_fraction(self):

        if self.instance_count > 0:
            return sum([res_usage / (self.instance_count * self.instance_max_usage[res_name]) for res_name, res_usage in self.system_resources_usage.items()]) / len(self.system_resources_usage)
        else:
            return 0

    def _comp(self, other_usage : 'SystemResourceUsage', comparison_op) -> bool:

        """ Implements common comparison logic """

        if not isinstance(other_usage, SystemResourceUsage):
            raise TypeError(f'Unexpected type of an operand when comparing with {self.__class__.__name__}: {other_usage.__class__.__name__}')

        return comparison_op(self.as_fraction(), other_usage.as_fraction())

    def __gt__(self, other_usage : 'SystemResourceUsage'):

        return self._comp(other_usage, operator.gt)

    def __ge__(self, other_usage : 'SystemResourceUsage'):

        return self._comp(other_usage, operator.ge)

    def __lt__(self, other_usage : 'SystemResourceUsage'):

        return self._comp(other_usage, operator.lt)

    def __le__(self, other_usage : 'SystemResourceUsage'):

        return self._comp(other_usage, operator.le)

    def __eq__(self, other_usage : 'SystemResourceUsage'):

        return self._comp(other_usage, operator.eq)

    def __ne__(self, other_usage : 'SystemResourceUsage'):

        return self._comp(other_usage, operator.ne)

    def copy(self):

        return self.__class__(self.node_info, self.instance_count,
                              self.system_resources_usage.copy())

    def __repr__(self):

        return f'{self.__class__.__name__}( node_info = {repr(self.node_info)}, \
                                            instance_count = {self.instance_count}, \
                                            system_resources_usage = {repr(self.system_resources_usage)})'
