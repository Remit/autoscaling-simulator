from abc import ABC, abstractmethod
import pandas as pd

from .score import Score

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

    _Registry = {}

    def __init__(self,
                 score_class : type):

        self.score_class = score_class

    @abstractmethod
    def __call__(self,
                 node_info : ScaledContainer,
                 duration : pd.Timedelta,
                 nodes_count : int) -> tuple:
        pass

    @classmethod
    def register(cls,
                 name : str):

        def decorator(score_calculator_class):
            cls._Registry[name] = score_calculator_class
            return score_calculator_class

        return decorator

    @classmethod
    def get(cls,
            name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use the non-existent score calculator {name}')

        return cls._Registry[name]

@ScoreCalculator.register('CostMinimizer')
class PriceScoreCalculator(ScoreCalculator):

    """
    Implements calculation of the score based on price.
    """

    def __init__(self):

        super().__init__(Score.get(self.__class__.__name__))

    def __call__(self,
                 node_info : ScaledContainer,
                 duration_h : float,
                 nodes_count : int) -> tuple:

        # TODO: consider taking cpu_credits_h into account
        price = duration_h * nodes_count * node_info.price_p_h
        score = self.score_class(price)

        return (score, price)
