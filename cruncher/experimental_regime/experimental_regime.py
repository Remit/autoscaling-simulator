from autoscalingsim import conf_keys
from abc import ABC, abstractmethod

class ExperimentalRegime(ABC):

    _Registry = {}

    _Policies_folders_names = [
        conf_keys.CONF_LOAD_MODEL_KEY,
        conf_keys.CONF_APPLICATION_MODEL_KEY,
        conf_keys.CONF_SCALING_POLICY_KEY,
        conf_keys.CONF_PLATFORM_MODEL_KEY,
        conf_keys.CONF_SCALING_MODEL_KEY,
        conf_keys.CONF_ADJUSTMENT_POLICY_KEY,
        conf_keys.CONF_DEPLOYMENT_MODEL_KEY,
        conf_keys.CONF_FAULT_MODEL_KEY
    ]

    _concretization_delimiter = '@@'
    _policies_categories_delimiter = '___'
    _simulation_instance_delimeter = '%%%'

    def __init__(self, simulator : 'Simulator', repetitions_count_per_simulation : int, keep_evaluated_configs : bool = False):

        self.simulator = simulator
        self.repetitions_count_per_simulation = repetitions_count_per_simulation
        self.keep_evaluated_configs = keep_evaluated_configs

    @abstractmethod
    def run_experiment(self, path_to_store_data : str):
        pass

    @classmethod
    def register(cls, name : str):

        def decorator(regime_class):
            cls._Registry[name] = regime_class
            return regime_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

from .regimes import *
