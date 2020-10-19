from .error_check import ErrorChecker

class ResourceRequirements:

    """
    Container for the resource requirements, e.g. by a service or a request.
    """

    def __init__(self,
                 requirements_raw : dict):

        self.vCPU = ErrorChecker.key_check_and_load('vCPU', requirements_raw)
        self.memory = ErrorChecker.key_check_and_load('memory', requirements_raw)
        self.disk = ErrorChecker.key_check_and_load('disk', requirements_raw)

    def to_dict(self):

        return {'vCPU': self.vCPU,
                'memory': self.memory,
                'disk': self.disk}
