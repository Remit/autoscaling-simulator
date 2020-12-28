from autoscalingsim.scaling.policiesbuilder.metric.scaling_aspect_calculation.desired_scaling_aspect_calculator import DesiredAspectValueCalculator
from autoscalingsim.utils.error_check import ErrorChecker

@DesiredAspectValueCalculator.register('learning')
class LearningBasedCalculator(DesiredAspectValueCalculator):

    _Registry = {}

    def __init__(self, config):

        super().__init__(config)

        # TODO: think about implementing an access point to the 1) cur metric val and 2) to quality metric
        target_value_raw = ErrorChecker.key_check_and_load('target_value', config)
        metric_unit_type = ErrorChecker.key_check_and_load('metric_unit_type', config)

    @classmethod
    def register(cls, name : str):

        def decorator(learning_based_calculator_class):
            cls._Registry[name] = learning_based_calculator_class
            return learning_based_calculator_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

from .linear import *
from .nonlinear import *
