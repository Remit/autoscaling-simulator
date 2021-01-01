import numbers

from .metric.metric_categories.size import Size
from .error_check import ErrorChecker

class ResourceRequirements: 

    """ Container for the resource requirements, e.g. by a service or a request """

    numeric_requirements = ['vCPU', 'memory', 'disk', 'network_bandwidth']

    @classmethod
    def new_empty_resource_requirements(cls):

        return cls(0, Size(0), Size(0), Size(0), [])

    @classmethod
    def from_coerced_dict(cls, requirements : dict):

        return cls(requirements['vCPU'], requirements['memory'], requirements['disk'],
                   requirements['network_bandwidth'], requirements['labels'])

    @classmethod
    def from_dict(cls, requirements_raw : dict):

        vCPU = ErrorChecker.key_check_and_load('vCPU', requirements_raw)

        memory_raw = ErrorChecker.key_check_and_load('memory', requirements_raw)
        memory_value = ErrorChecker.key_check_and_load('value', memory_raw)
        memory_unit = ErrorChecker.key_check_and_load('unit', memory_raw)
        memory = Size(memory_value, memory_unit)

        disk_raw = ErrorChecker.key_check_and_load('disk', requirements_raw)
        disk_value = ErrorChecker.key_check_and_load('value', disk_raw)
        disk_unit = ErrorChecker.key_check_and_load('unit', disk_raw)
        disk = Size(disk_value, disk_unit)

        try:
            network_bandwidth_raw = ErrorChecker.key_check_and_load('network_bandwidth', requirements_raw)
            network_bandwidth_value = ErrorChecker.key_check_and_load('value', network_bandwidth_raw)
            network_bandwidth_unit = ErrorChecker.key_check_and_load('unit', network_bandwidth_raw)
            network_bandwidth = Size(network_bandwidth_value, network_bandwidth_unit)
        except AttributeError:
            network_bandwidth = Size(0)

        try:
            labels = ErrorChecker.key_check_and_load('labels', requirements_raw)
        except ValueError:
            labels = []

        return cls(vCPU, memory, disk, network_bandwidth, labels)

    def __init__(self, vCPU : int, memory : Size, disk : Size,
                 network_bandwidth : Size, labels : list):

        self.vCPU = vCPU if not vCPU is None else 0
        self.memory = memory if not memory is None else Size(0)
        self.disk = disk if not disk is None else Size(0)
        self.network_bandwidth = network_bandwidth if not network_bandwidth is None else Size(0)
        self.labels = labels if not labels is None else list()

    def copy(self):

        return self.__class__(self.vCPU, self.memory, self.disk, self.network_bandwidth, self.labels)

    def to_dict(self):

        return {'vCPU': self.vCPU, 'memory': self.memory,
                'disk': self.disk, 'network_bandwidth': self.network_bandwidth,
                'labels': self.labels}

    def tracked_resources(self):

        return ['vCPU', 'memory', 'disk', 'network_bandwidth']

    def __add__(self, other_res_req : 'ResourceRequirements'):

        sum_res_req_dict = self.to_dict()
        for req_name, req_val in other_res_req.to_dict().items():
            if not req_name in sum_res_req_dict:
                sum_res_req_dict[req_name] = req_val
            else:
                sum_res_req_dict[req_name] += req_val

        return self.__class__.from_coerced_dict(sum_res_req_dict)

    def __radd__(self, other_res_req : 'ResourceRequirements'):

        return self.__add__(other_res_req)

    def __mul__(self, factor : numbers.Number):

        new_res_req_dict = dict()
        for req_name, req_val in self.to_dict().items():
            if req_name in self.__class__.numeric_requirements:
                new_res_req_dict[req_name] = req_val * factor
            else:
                new_res_req_dict[req_name] = req_val #correct?

        return self.__class__.from_coerced_dict(new_res_req_dict)

    def __rmul__(self, factor : numbers.Number):

        return self.__mul__(factor)

    def __repr__(self):

        return f'{self.__class__.__name__}(vCPU = {self.vCPU},\
                                           memory = {self.memory},\
                                           disk = {self.disk}, \
                                           network_bandwidth = {self.network_bandwidth},\
                                           labels = {self.labels})'
