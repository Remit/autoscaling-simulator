from ..utils.state.capacity import Capacity
from ..scaling.policiesbuilder.scaled.scaled_container import ScaledContainer

class SystemCapacity(Capacity):

    """
    Wraps the system capacity taken, i.e. system resources that are taken such
    as CPU, memory, and network bandwidth. Since it is bound to the capacity
    of a particular node type, a check of node type is implemented on arithmetic
    operations with capacity.
    """

    layout = ['vCPU',
              'memory',
              'network_bandwidth_MBps']

    def __init__(self,
                 container_info : ScaledContainer,
                 instance_count : int = 1,
                 system_capacity_taken : dict = {'vCPU' : 0,
                                                 'memory' : 0,
                                                 'network_bandwidth_MBps' : 0}):

        self.container_info = container_info

        self.uid = container_info.get_unique_id()
        self.instance_capacity = container_info.get_capacity()
        self.instance_count = instance_count

        self.system_capacity_taken = {}
        for cap_type in SystemCapacity.layout:
            if not cap_type in system_capacity_taken:
                self.system_capacity_taken[cap_type] = 0
            else:
                self.system_capacity_taken[cap_type] = system_capacity_taken[cap_type]

    def __add__(self,
                cap_to_add : 'SystemCapacity'):

        if not isinstance(cap_to_add, self.__class__):
            raise ValueError('An attempt to add an object of type {} to the object of type {}'.format(cap_to_add.__class__.__name__, self.__class__.__name__))

        if self.uid != cap_to_add.uid:
            raise ValueError('An attempt to add capacities with different unique IDs: {} and {}'.format(self.uid, cap_to_add.uid))

        sum_system_capacity = {}
        for self_cap, other_cap in zip(self.system_capacity_taken, cap_to_add.system_capacity_taken):
            sum_system_capacity[self_cap[0]] = self_cap[1] + other_cap[1]

        return SystemCapacity(self.container_info,
                              self.instance_count,
                              sum_system_capacity)

    def __sub__(self,
                cap_to_sub : 'SystemCapacity'):

        if not isinstance(cap_to_sub, self.__class__):
            raise ValueError('An attempt to subtract an object of type {} to the object of type {}'.format(cap_to_sub.__class__.__name__, self.__class__.__name__))

        if self.uid != cap_to_sub.uid:
            raise ValueError('An attempt to subtract capacities with different unique IDs: {} and {}'.format(self.uid, cap_to_sub.uid))

        reduced_system_capacity = {}
        for self_cap, other_cap in zip(self.system_capacity_taken, cap_to_sub.system_capacity_taken):
            reduced_system_capacity[self_cap[0]] = self_cap[1] - other_cap[1]

        return SystemCapacity(self.container_info,
                              self.instance_count,
                              reduced_system_capacity)

    def __mul__(self,
                scalar : int):

        if not isinstance(scalar, int):
            raise ValueError('An attempt to mutiply the {} by non-int'.format(self.__class__.__name__))

        mult_system_capacity = {}
        for cap_name, self_cap in self.system_capacity_taken.items():
            mult_system_capacity[cap_name] = scalar * self_cap

        return SystemCapacity(self.container_info,
                              self.instance_count,
                              mult_system_capacity)

    def is_exhausted(self):

        """
        Checking whether the system capacity is exhausted.
        """

        for sys_cap_type, sys_cap_taken in self.system_capacity_taken.items():
            if sys_cap_taken >= self.instance_count * self.instance_capacity[sys_cap_type]:
                return True

        return False

    def is_empty(self):

        """
        Checking whether no capacity is taken.
        """

        for sys_cap_type, sys_cap_taken in self.system_capacity_taken.items():
            if sys_cap_taken > 0:
                return False

        return True

    def to_dict(self):

        return self.system_capacity_taken

    def normalized_capacity_consumption(self,
                                        capacity_type : str):

        if capacity_type in self.system_capacity_taken:
            return self.system_capacity_taken[capacity_type] / (self.instance_count * self.instance_capacity[capacity_type])
        else:
            return 0

# TODO: consider removing?where is it used?
    def collapse(self):

        joint_capacity = 0.0
        for sys_cap_type, sys_cap in self.system_capacity_taken.items():
            joint_capacity += (sys_cap / (self.instance_count * self.instance_capacity[sys_cap_type]))

        joint_capacity /= len(self.system_capacity_taken)
        return joint_capacity
