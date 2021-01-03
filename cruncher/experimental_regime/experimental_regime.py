from abc import ABC, abstractmethod

class ExperimentalRegime(ABC):

    _Registry = {}

    def __init__(self, simulator : 'Simulator', repetitions_count_per_simulation : int, results_folder : str):

        self.simulator = simulator
        self.repetitions_count_per_simulation = repetitions_count_per_simulation
        self.results_folder = results_folder

    @abstractmethod
    def run_experiment(self):
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
