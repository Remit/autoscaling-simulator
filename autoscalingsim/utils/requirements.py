import numbers

from .error_check import ErrorChecker

class ResourceRequirements:

    """
    Container for the resource requirements, e.g. by a service or a request.
    """

    @classmethod
    def new_empty_resource_requirements(cls):

        return cls({'vCPU': 0,
                    'memory': 0,
                    'disk': 0,
                    'labels': []})

    def __init__(self,
                 requirements_raw : dict):

        self.vCPU = ErrorChecker.key_check_and_load('vCPU', requirements_raw)
        self.memory = ErrorChecker.key_check_and_load('memory', requirements_raw)
        self.disk = ErrorChecker.key_check_and_load('disk', requirements_raw)
        try:
            self.labels = ErrorChecker.key_check_and_load('labels', requirements_raw)
        except ValueError:
            self.labels = []

    def to_dict(self):

        return {'vCPU': self.vCPU,
                'memory': self.memory,
                'disk': self.disk,
                'labels': self.labels}

    def tracked_resources(self):

        return ['vCPU', 'memory', 'disk']

    def __add__(self,
                other_res_req : 'ResourceRequirements'):

        if not isinstance(other_res_req, ResourceRequirements):
            raise TypeError(f'Unrecognized type to add to the {self.__class__.__name__}: {other_res_req.__class__.__name__}')

        sum_res_req_dict = self.to_dict()
        for req_name, req_val in other_res_req.to_dict().items():
            if not req_name in sum_res_req_dict:
                sum_res_req_dict[req_name] = req_val
            else:
                sum_res_req_dict[req_name] += req_val

        return ResourceRequirements(sum_res_req_dict)

    def __radd__(self,
                 other_res_req : 'ResourceRequirements'):

        return self.__add__(other_res_req)

    def __mul__(self,
                factor : numbers.Number):

        if not isinstance(factor, numbers.Number):
            raise TypeError(f'An attempt to multiply {self.__class__.__name__} by non-scalar: {factor.__class__.__name__}')

        new_res_req_dict = self.to_dict()
        for req_name, req_val in new_res_req_dict.items():
            if isinstance(req_val, numbers.Number):
                new_res_req_dict[req_name] *= factor

        return ResourceRequirements(new_res_req_dict)

    def __rmul__(self,
                 factor : numbers.Number):

        return self.__mul__(factor)
