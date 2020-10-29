from abc import ABC, abstractmethod
import pandas as pd

from . import score

from ...scaled.scaled_container import ScaledContainer

class ScoreCalculator(ABC):

    """
    Provides an interface to various score calaculators.
    Should return a score and a parameter estimate upon call.
    The property of the score is as follows: higher values of score are
    more desirable from the point of view of optimization. For instance,
    if we want to optimize for the price, then the score should be either
    reverse of the price or negative.
    """

    def __init__(self,
                 score_class : type):

        self.score_class = score_class

    @abstractmethod
    def __call__(self,
                 container_info : ScaledContainer,
                 duration : pd.Timedelta,
                 containers_count : int) -> tuple:
        pass

class PriceScoreCalculator(ScoreCalculator):

    """
    Implements calculation of the score based on price.
    """

    def __init__(self):

        super().__init__(score.Registry.get(self.__class__.__name__))

    def __call__(self,
                 container_info : ScaledContainer,
                 duration_h : float,
                 containers_count : int) -> tuple:

        # TODO: consider taking cpu_credits_h into account
        price = duration_h * containers_count * container_info.price_p_h
        score = self.score_class(price)

        return (score, price)

class Registry:

    """
    Stores the calculator classes and organizes access to them.
    """

    registry = {
        'CostMinimizer': PriceScoreCalculator
    }

    @staticmethod
    def get(name):

        if not name in Registry.registry:
            raise ValueError(f'An attempt to use the non-existent score calculator {name}')

        return Registry.registry[name]
