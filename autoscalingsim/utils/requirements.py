import numbers
import numpy as np
import collections
from copy import deepcopy

from autoscalingsim.infrastructure_platform.node_information.system_resource_usage import SystemResourceUsage
from .metric.metric_categories.numeric import Numeric
from .metric.metric_categories.size import Size
from .error_check import ErrorChecker

class CountableResourceRequirement:

    """ Represents countable system resource requirement generating samples of resource requirement from the normal distribution """

    countable_requirements = {
        'vCPU' : Numeric,
        'memory' : Size,
        'disk' : Size,
        'network_bandwidth' : Size
    }

    _sanity_coef = 0.001 # fraction of mean considered as a minimal unit for the resource requirement

    @classmethod
    def get_resource_class(cls, resource_name : str):

        return cls.countable_requirements[resource_name]

    @classmethod
    def vCPU_from_dict(cls, config : dict = None):

        return cls.from_dict(cls.get_resource_class('vCPU'), config)

    @classmethod
    def memory_from_dict(cls, config : dict = None):

        return cls.from_dict(cls.get_resource_class('memory'), config)

    @classmethod
    def disk_from_dict(cls, config : dict = None):

        return cls.from_dict(cls.get_resource_class('disk'), config)

    @classmethod
    def network_bandwidth_from_dict(cls, config : dict = None):

        return cls.from_dict(cls.get_resource_class('network_bandwidth'), config)

    @classmethod
    def from_dict(cls, resource_class : type, config : dict = None):

        mean = ErrorChecker.key_check_and_load('mean', config, default = 0)
        std = ErrorChecker.key_check_and_load('std', config, default = 0)
        unit = ErrorChecker.key_check_and_load('unit', config, default = resource_class.default_unit)

        return cls(resource_class, mean, std, unit)

    @classmethod
    def lim_vCPU_from_dict(cls, config : dict = None):

        return cls.limit_from_dict(cls.get_resource_class('vCPU'), config)

    @classmethod
    def lim_memory_from_dict(cls, config : dict = None):

        return cls.limit_from_dict(cls.get_resource_class('memory'), config)

    @classmethod
    def lim_disk_from_dict(cls, config : dict = None):

        return cls.limit_from_dict(cls.get_resource_class('disk'), config)

    @classmethod
    def lim_network_bandwidth_from_dict(cls, config : dict = None):

        return cls.limit_from_dict(cls.get_resource_class('network_bandwidth'), config)

    @classmethod
    def limit_from_dict(cls, resource_class : type, config : dict = None):

        val = ErrorChecker.key_check_and_load('value', config, default = 0)
        unit = ErrorChecker.key_check_and_load('unit', config, default = resource_class.default_unit)

        return resource_class(val, unit = unit)

    def __init__(self, resource_class : type, mean : float = 0, std : float = 0, unit : str = None):

        self._resource_class = resource_class
        self._mean = self._resource_class(mean, unit)
        self._std = self._resource_class(std, unit)

    def __add__(self, other : 'CountableResourceRequirement'):

        if self._has_comparable_resource_class_with(other):

            if self.is_empty:
                return deepcopy(other)

            elif other.is_empty:
                return deepcopy(self)

            else:

                return self.__class__(self._resource_class,
                                      self._mean.value + other._mean.value / 2.0,
                                      self._std.value + other._std.value / 2.0)

    def __mul__(self, factor : numbers.Number):

        return self.__class__(self._resource_class, self._mean.value * factor, self._std.value * factor)

    @property
    def mean(self):

        return self._mean

    @property
    def is_empty(self):

        return self._mean.value == 0 and self._std.value == 0

    @property
    def sample(self):

        return self._resource_class(max(np.random.normal(self._mean.value, self._std.value), self.__class__._sanity_coef * self._mean.value))

    def _has_comparable_resource_class_with(self, other : 'CountableResourceRequirement'):

        if self._resource_class != other._resource_class:
            raise ValueError(f'An attempt to combine uncomparable resource classes: {self._resource_class.__name__} and {other._resource_class.__name__}')

        return True

    def __repr__(self):

        return f'{self.__class__.__name__}(resource_class = {self._resource_class}, \
                                           mean = {self._mean.value}, \
                                           std = {self._std.value})'

class ResourceRequirements:

    """ Container for the resource requirements, e.g. by a service or a request """

    @classmethod
    def from_dict(cls, requirements_raw : dict):

        vCPU = CountableResourceRequirement.vCPU_from_dict(ErrorChecker.key_check_and_load('vCPU', requirements_raw))
        memory = CountableResourceRequirement.memory_from_dict(ErrorChecker.key_check_and_load('memory', requirements_raw))
        disk = CountableResourceRequirement.disk_from_dict(ErrorChecker.key_check_and_load('disk', requirements_raw))

        try:
            network_bandwidth = CountableResourceRequirement.network_bandwidth_from_dict(ErrorChecker.key_check_and_load('network_bandwidth', requirements_raw))
        except AttributeError:
            network_bandwidth = CountableResourceRequirement.network_bandwidth_from_dict()

        limits = ErrorChecker.key_check_and_load('limits', requirements_raw, default = dict())
        lim_vCPU, lim_memory, lim_disk, lim_network_bandwidth = None, None, None, None
        if isinstance(limits, collections.Mapping):
            if len(limits) > 0:
                lim_vCPU = CountableResourceRequirement.lim_vCPU_from_dict(ErrorChecker.key_check_and_load('vCPU', limits))
                lim_memory = CountableResourceRequirement.lim_memory_from_dict(ErrorChecker.key_check_and_load('memory', limits))
                lim_disk = CountableResourceRequirement.lim_disk_from_dict(ErrorChecker.key_check_and_load('disk', limits))
                try:
                    lim_network_bandwidth = CountableResourceRequirement.lim_network_bandwidth_from_dict(ErrorChecker.key_check_and_load('network_bandwidth', limits))
                except AttributeError:
                    lim_network_bandwidth = CountableResourceRequirement.lim_network_bandwidth_from_dict()

        else: # Coefficient case
            lim_vCPU = vCPU.mean * limits
            lim_memory = memory.mean * limits
            lim_disk = disk.mean * limits
            lim_network_bandwidth = network_bandwidth.mean * limits

        try:
            labels = ErrorChecker.key_check_and_load('labels', requirements_raw)
        except ValueError:
            labels = []

        return cls(vCPU, memory, disk, network_bandwidth, lim_vCPU, lim_memory, lim_disk, lim_network_bandwidth, labels)

    def __init__(self,
                 vCPU : CountableResourceRequirement = None, memory : CountableResourceRequirement = None,
                 disk : CountableResourceRequirement = None, network_bandwidth : CountableResourceRequirement = None,
                 lim_vCPU : Numeric = None, lim_memory : Size = None, lim_disk : Size = None, lim_network_bandwidth : Size = None,
                 labels : list = None):

        self._vCPU = vCPU if not vCPU is None else CountableResourceRequirement.vCPU_from_dict()
        self._memory = memory if not memory is None else CountableResourceRequirement.memory_from_dict()
        self._disk = disk if not disk is None else CountableResourceRequirement.disk_from_dict()
        self._network_bandwidth = network_bandwidth if not network_bandwidth is None else CountableResourceRequirement.network_bandwidth_from_dict()

        self._lim_vCPU = lim_vCPU if not lim_vCPU is None else CountableResourceRequirement.lim_vCPU_from_dict()
        self._lim_memory = lim_memory if not lim_memory is None else CountableResourceRequirement.lim_memory_from_dict()
        self._lim_disk = lim_disk if not lim_disk is None else CountableResourceRequirement.lim_disk_from_dict()
        self._lim_network_bandwidth = lim_network_bandwidth if not lim_network_bandwidth is None else CountableResourceRequirement.lim_network_bandwidth_from_dict()

        self._labels = labels if not labels is None else list()

    @property
    def labels(self):

        return self._labels.copy()

    @property
    def limits(self):

        return {'vCPU': self._lim_vCPU, 'memory': self._lim_memory,
                'disk': self._lim_disk, 'network_bandwidth': self._lim_network_bandwidth,
                'labels': self._labels}

    @property
    def limits_sample(self):

        return ResourceRequirementsSample(**self.limits)

    @property
    def average_representation(self):

        return {'vCPU': self._vCPU.mean, 'memory': self._memory.mean,
                'disk': self._disk.mean, 'network_bandwidth': self._network_bandwidth.mean,
                'labels': self._labels}

    @property
    def average_sample(self):

        return ResourceRequirementsSample(**self.average_representation)

    @property
    def sample(self):

        return ResourceRequirementsSample(self._vCPU.sample, self._memory.sample, self._disk.sample, self._network_bandwidth.sample, self._labels.copy())

    def tracked_resources(self):

        return list(self.__class__.countable_requirements.keys())

    def __add__(self, other : 'ResourceRequirements'):

        return self.__class__(self._vCPU + other._vCPU, self._memory + other._memory,
                              self._disk + other._disk, self._network_bandwidth + other._network_bandwidth,
                              self._lim_vCPU + other._lim_vCPU, self._lim_memory + other._lim_memory,
                              self._lim_disk + other._lim_disk, self._lim_network_bandwidth + other._lim_network_bandwidth,
                              self._labels + other._labels)

    def __radd__(self, other : 'ResourceRequirements'):

        if other == 0:
            return deepcopy(self)

        return self.__add__(other)

    def __mul__(self, factor : numbers.Number):

        return self.__class__(self._vCPU * factor, self._memory * factor,
                              self._disk * factor, self._network_bandwidth * factor,
                              self._lim_vCPU * factor, self._lim_memory * factor,
                              self._lim_disk * factor, self._lim_network_bandwidth * factor,
                              self._labels.copy())

    def __rmul__(self, factor : numbers.Number):

        return self.__mul__(factor)

    def how_many_nodes_needed(self, node_info : 'NodeInfo', service_inst_cnt : int, service_inst_res_usage):

        res_estimate_total = max(np.ceil(self._vCPU.mean / (node_info._vCPU * SystemResourceUsage.MAX_ALLOWED_VCPU_SHARING_FACTOR)),
                                 np.ceil(self._memory.mean / node_info._memory),
                                 np.ceil(self._disk.mean / node_info._disk),
                                 np.ceil(self._network_bandwidth.mean / node_info._network_bandwidth))

        res_estimate_service_only = max(min(np.ceil(service_inst_res_usage._vCPU.mean / node_info._vCPU * SystemResourceUsage.MAX_ALLOWED_VCPU_SHARING_FACTOR * SystemResourceUsage.MAX_ALLOWED_SYSTEM_RESOURCE_USAGE_BY_SERVICES),
                                            np.ceil(service_inst_res_usage._memory.mean / node_info._memory * SystemResourceUsage.MAX_ALLOWED_SYSTEM_RESOURCE_USAGE_BY_SERVICES),
                                            np.ceil(service_inst_res_usage._disk.mean / node_info._disk * SystemResourceUsage.MAX_ALLOWED_SYSTEM_RESOURCE_USAGE_BY_SERVICES),
                                            np.ceil(service_inst_res_usage._network_bandwidth.mean / node_info._network_bandwidth * SystemResourceUsage.MAX_ALLOWED_SYSTEM_RESOURCE_USAGE_BY_SERVICES)), 1)

        #service_instances_estimate = np.ceil(service_inst_cnt / (node_info._vCPU * SystemResourceUsage.MAX_ALLOWED_VCPU_SHARING_FACTOR).value)

        return int(max(res_estimate_total, res_estimate_service_only))

    def __repr__(self):

        return f'{self.__class__.__name__}(vCPU = {self._vCPU},\
                                           memory = {self._memory},\
                                           disk = {self._disk}, \
                                           network_bandwidth = {self._network_bandwidth},\
                                           lim_vCPU = {self._lim_vCPU},\
                                           lim_memory = {self._lim_memory},\
                                           lim_disk = {self._lim_disk}, \
                                           lim_network_bandwidth = {self._lim_network_bandwidth},\
                                           labels = {self._labels})'

class ResourceRequirementsSample:

    def __init__(self, vCPU : Numeric = None, memory : Size = None, disk : Size = None, network_bandwidth : Size = None, labels : list = list()):

        self.vCPU = vCPU if not vCPU is None else CountableResourceRequirement.get_resource_class('vCPU')(0)
        self.memory = memory if not memory is None else CountableResourceRequirement.get_resource_class('memory')(0)
        self.disk = disk if not disk is None else CountableResourceRequirement.get_resource_class('disk')(0)
        self.network_bandwidth = network_bandwidth if not network_bandwidth is None else CountableResourceRequirement.get_resource_class('network_bandwidth')(0)
        self.labels = labels if not labels is None else list()

    def __add__(self, other : 'ResourceRequirementsSample'):

        return self.__class__(self.vCPU + other.vCPU, self.memory + other.memory, self.disk + other.disk,
                              self.network_bandwidth + other.network_bandwidth, self.labels + other.labels)

    def __mul__(self, factor : numbers.Number):

        return self.__class__(self.vCPU * factor, self.memory * factor, self.disk * factor, self.network_bandwidth * factor, self.labels.copy())

    def to_dict(self):

        return {'vCPU': self.vCPU, 'memory': self.memory,
                'disk': self.disk, 'network_bandwidth': self.network_bandwidth,
                'labels': self.labels}

    def __repr__(self):

        return f'{self.__class__.__name__}(vCPU = {self.vCPU}, \
                                           memory = {self.memory}, \
                                           disk = {self.disk}, \
                                           network_bandwidth = {self.network_bandwidth}, \
                                           labels = {self.labels})'
