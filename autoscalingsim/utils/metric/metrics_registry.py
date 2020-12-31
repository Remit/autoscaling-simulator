from .metric_categories import *

class MetricsRegistry:

    _Types_registry = {
        'buffer_time': Duration,
        'response_time': Duration,
        'network_time': Duration,
        'vCPU': Numeric,
        'memory': Size,
        'disk': Size,
        'network_bandwidth': Size,
        'load': Rate
    }

    @classmethod
    def get(cls, name : str):

        return cls._Types_registry[name] if name in cls._Types_registry else Numeric
