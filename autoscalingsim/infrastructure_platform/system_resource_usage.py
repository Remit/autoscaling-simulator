import operator
import numbers

from ..utils.state.usage import Usage
from ..scaling.policiesbuilder.scaled.scaled_container import ScaledContainer

class SystemResourceUsage(Usage):

    """
    Represents system resources usage, e.g. vCPU, memory, and network bandwidth.
    Since it is bound to the system resources available on a particular node type,
    each arithmetic operation on the system resource usage checks whether the
    operands have the matching node types.
    """

    system_resources = ['vCPU', 'memory', 'disk', 'network_bandwidth_MBps']

    def __init__(self,
                 node_info : ScaledContainer,
                 instance_count : int = 1,
                 system_resources_usage : dict = {'vCPU' : 0, 'memory' : 0, 'disk': 0,
                                                  'network_bandwidth_MBps' : 0}):

        self.node_info = node_info

        self.uid = node_info.get_unique_id()
        self.instance_max_usage = node_info.get_max_usage()
        self.instance_count = instance_count

        self.system_resources_usage = {}
        for res_type in self.__class__.system_resources:
            if not res_type in system_resources_usage:
                self.system_resources_usage[res_type] = 0
            else:
                self.system_resources_usage[res_type] = system_resources_usage[res_type]

    def __add__(self, usage_to_add : 'SystemResourceUsage'):

        return self._add(usage_to_add, 1)

    def __sub__(self, usage_to_sub : 'SystemResourceUsage'):

        return self._add(usage_to_sub, -1)

    def _add(self, other_usage : 'SystemResourceUsage', sign : int = 1):

        if not isinstance(other_usage, self.__class__):
            raise ValueError(f'An attempt to combine an object of type {other_usage.__class__.__name__} with an object of type {self.__class__.__name__}')

        if self.uid != other_usage.uid:
            raise ValueError(f'An attempt to combine resource usages belonging to different node types: {self.uid} and {other_usage.uid}')

        system_resource_usage = self.system_resources_usage.copy()
        for res_name, res_usage in other_usage.system_resources_usage.items():
            if res_name in system_resource_usage:
                system_resource_usage[res_name] += sign * res_usage

        return self.__class__(self.node_info, self.instance_count, system_resource_usage)

    def __mul__(self, multiplier : float):

        if not isinstance(multiplier, numbers.Number):
            raise ValueError(f'An attempt to mutiply the {self.__class__.__name__} by non-number')

        system_resource_usage = {res_name : res_usage * multiplier for res_name, res_usage in self.system_resources_usage.items()}

        return self.__class__(self.node_info, self.instance_count, system_resource_usage)

    def _comp(self, other_usage : 'SystemResourceUsage', comparison_op) -> bool:

        """ Implements common comparison logic """

        if not isinstance(other_usage, SystemResourceUsage):
            raise TypeError(f'Unexpected type of an operand when comparing with {self.__class__.__name__}: {other_usage.__class__.__name__}')

        return comparison_op(self.collapse(), other_usage.collapse())

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

        return f'{self.__class__.__name__}({repr(self.node_info)}, \
                 {self.instance_count}, \
                 {repr(self.system_resources_usage)})'

    def is_full(self):

        """ Checks whether the system resources are exhausted """

        for res_name, res_usage in self.system_resources_usage.items():
            if res_usage > self.instance_count * self.instance_max_usage[res_name]:
                return True

        return False

    def is_zero(self):

        """ Checks whether at least something takes system resources """

        for res_name, res_usage in self.system_resources_usage.items():
            if res_usage > 0:
                return False

        return True

    def to_dict(self):

        return self.system_resources_usage.copy()

    def normalized_usage(self, res_name : str):

        if res_name in self.system_resources_usage:
            return 0 if self.instance_count == 0 else self.system_resources_usage[res_name] / (self.instance_count * self.instance_max_usage[res_name])
        else:
            return 0

    def collapse(self):

        return sum([res_usage / (self.instance_count * self.instance_max_usage[res_name]) for res_name, res_usage in self.system_resources_usage.items()]) / len(self.system_resources_usage)
