from abc import ABC, abstractmethod
import pandas as pd

from .score import Score

from autoscalingsim.infrastructure_platform.node_information.node import NodeInfo

class ScoreCalculator(ABC):

    _Registry = {}

    def __init__(self, score_class : type):

        self.score_class = score_class

    @abstractmethod
    def compute_score(self, node_info : NodeInfo, duration : pd.Timedelta, nodes_count : int) -> tuple:

        pass

    @abstractmethod
    def build_init_score(self):

        pass

    @classmethod
    def register(cls, name : str):

        def decorator(score_calculator_class):
            cls._Registry[name] = score_calculator_class
            return score_calculator_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {self.__class__.__name__} {name}')

        return cls._Registry[name]

from .score_calculator_impl import *
