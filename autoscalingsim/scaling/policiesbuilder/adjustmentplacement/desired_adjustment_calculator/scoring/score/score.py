import operator
import math
from abc import ABC, abstractmethod

class Score(ABC):

    _Registry = {}

    @classmethod
    def create_worst_score(cls):

        return cls(None, None)

    def __init__(self, score_computer, score_inverter):

        self.score_computer = score_computer
        self.score_inverter = score_inverter
        self.score = 0

    @abstractmethod
    def __add__(self, other):

        pass

    @abstractmethod
    def __mul__(self, other):

        pass

    @abstractmethod
    def __truediv__(self, other):

        pass

    @classmethod
    @abstractmethod
    def build_init_score(self):

        pass

    @classmethod
    @abstractmethod
    def build_worst_score(self):

        pass

    def __lt__(self, other : 'Score'):

        return self._compare(other, operator.lt)

    def __le__(self, other : 'Score'):

        return self._compare(other, operator.le)

    def __gt__(self, other : 'Score'):

        return self._compare(other, operator.gt)

    def __ge__(self, other : 'Score'):

        return self._compare(other, operator.ge)

    def __eq__(self, other : 'Score'):

        return self._compare(other, operator.eq)

    def __ne__(self, other : 'Score'):

        return self._compare(other, operator.ne)

    def _compare(self, other : 'Score', comparison_op):

        return comparison_op(self.score, other.score)

    @property
    def original_value(self):

        return self.score_inverter(self.score)

    @property
    def is_worst(self):

        return self.score == 0

    @property
    def is_finite(self):

        """ Checks whether such a score makes sense, i.e. no infinetely appealing scores are allowed """

        return (not math.isinf(self.score))

    @classmethod
    def register(cls, name : str):

        def decorator(score_class):
            cls._Registry[name] = score_class
            return score_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

from .score_impl import *
