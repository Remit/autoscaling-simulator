from ..utils.capacity import Capacity

class SystemCapacity(Capacity):

    """
    Wraps the system capacity taken, i.e. system resources that are taken such
    as CPU, memory, and network bandwidth. Since it is normalized to the capacity
    of a particular node type, a check of node type is implemented on arithmetic
    operations with capacity.
    """

    def __init__(self,
                 node_type,
                 system_capacity):

        self.node_type = node_type
        self.system_capacity = system_capacity

    def __init__(self,
                 node_type,
                 vCPU = 0,
                 memory = 0,
                 network_bandwidth_MBps = 0):

        system_capacity = {}
        system_capacity['vCPU'] = vCPU
        system_capacity['memory'] = memory
        system_capacity['network_bandwidth_MBps'] = network_bandwidth_MBps

        self.__init__(node_type,
                      system_capacity)

    def __add__(self,
                cap_to_add):

        if not isinstance(cap_to_add, self.__class__):
            raise ValueError('An attempt to add an object of type {} to the object of type {}'.format(cap_to_add.__class__.__name__, self.__class__.__name__))

        if self.node_type != cap_to_add.node_type:
            raise ValueError('An attempt to add capacities for different node types: {} and {}'.format(self.node_type, cap_to_add.node_type))

        sum_system_capacity = {}
        for self_cap, other_cap in zip(self.system_capacity, cap_to_add.system_capacity):
            sum_system_capacity[self_cap[0]] = self_cap[1] + other_cap[1]

        return SystemCapacity(self.node_type,
                              sum_system_capacity)

    def __sub__(self,
                cap_to_sub):

        if not isinstance(cap_to_sub, self.__class__):
            raise ValueError('An attempt to subtract an object of type {} to the object of type {}'.format(cap_to_sub.__class__.__name__, self.__class__.__name__))

        if self.node_type != cap_to_sub.node_type:
            raise ValueError('An attempt to subtract capacities for different node types: {} and {}'.format(self.node_type, cap_to_sub.node_type))

        red_system_capacity = {}
        for self_cap, other_cap in zip(self.system_capacity, cap_to_sub.system_capacity):
            red_system_capacity[self_cap[0]] = self_cap[1] - other_cap[1]

        return SystemCapacity(self.node_type,
                              red_system_capacity)

    def __mul__(self,
                scalar):

        if not isinstance(scalar, int):
            raise ValueError('An attempt to mutiply the {} by non-int'.format(self.__class__.__name__))

        mult_system_capacity = {}
        for cap_name, self_cap in self.system_capacity.items():
            mult_system_capacity[cap_name] = scalar * self_cap

        return SystemCapacity(self.node_type,
                              mult_system_capacity)

    def is_exhausted(self):

        """
        Checking whether the system capacity is exhausted.
        """

        for sys_cap_type, sys_cap in self.system_capacity:
            if sys_cap > 1:
                return True

        return False

    def is_empty(self):

        """
        Checking whether no capacity is taken.
        """

        for sys_cap_type, sys_cap in self.system_capacity:
            if sys_cap < 0:
                return True

        return False

    def collapse(self):

        joint_capacity = 0.0
        for sys_cap_type, sys_cap in self.system_capacity:
            joint_capacity += sys_cap

        joint_capacity /= len(self.system_capacity)
        return joint_capacity
