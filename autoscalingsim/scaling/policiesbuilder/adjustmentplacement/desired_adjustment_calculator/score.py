import numbers
import math
from abc import ABC, abstractmethod

from autoscalingsim.utils.functions import InvertingFunction

class StateScore:

    """
    Wraps scores for state on per region basis.
    """

    def __init__(self,
                 scores_per_region : dict):

        self.scores_per_region = scores_per_region

    def __add__(self,
                other_state_score : 'StateScore'):

        if not isinstance(other_state_score, StateScore):
            raise TypeError(f'Unexpected type to add to {self.__class__.__name__}: {other_state_score.__class__.__name__}')

        scores_per_region = self.scores_per_region
        for region_name, other_region_score in other_state_score.scores_per_region.items():
            if not region_name in scores_per_region:
                scores_per_region[region_name] = other_region_score
            else:
                scores_per_region[region_name] += other_region_score

        return self.__class__(scores_per_region)

    def __mul__(self,
                state_duration):

        scores_per_region = {}
        if isinstance(state_duration, StateDuration):
            for region_name, score in self.scores_per_region.items():
                if region_name in state_duration.durations_per_region_h:
                    scores_per_region[region_name] = score * state_duration.durations_per_region_h[region_name]
        elif isinstance(state_duration, numbers.Number):
            for region_name, score in self.scores_per_region.items():
                scores_per_region[region_name] = score * state_duration
        else:
            raise TypeError(f'An attempt to multiply {self.__class__.__name__} by an unknown type: {state_duration.__class__.__name__}')

        return StateScore(self.scores_per_region)

    def __truediv__(self,
                    scalar : numbers.Number):

        if not isinstance(scalar, numbers.Number):
            raise TypeError(f'An attempt to divide {self.__class__.__name__} by an unknown type: {scalar.__class__.__name__}')

        if scalar <= 0:
            raise ValueError(f'An attempt to divide the {self.__class__.__name__} by a negative number or zero')

        new_scores_per_region = {}
        for region_name, score in self.scores_per_region.items():
            new_scores_per_region[region_name] = score / scalar

        return StateScore(new_scores_per_region)

    def __iter__(self):

        return StateScoreIterator(self)

    def collapse(self):

        """
        Produces joint score for the whole state without distinguishing between
        regions.
        """

        joint_score = None
        if len(self.scores_per_region) > 0:
            scores_lst = list(self.scores_per_region.values())
            joint_score = sum(scores_lst, type(scores_lst[0])(0))

            if (not joint_score.is_sane()) or (joint_score is None):
                return type(scores_lst[0])(float('Inf'))
            else:
                return joint_score

        else:
            raise ValueError(f'Empty {self.__class__.__name__} to collapse')

    def score_for_region(self, region_name : str):

        if not region_name in self.scores_per_region:
            raise ValueError(f'Unexpected region name {region_name}')

        return self.scores_per_region[region_name]

    @property
    def regions(self):

        return list(self.scores_per_region.keys())

class StateScoreIterator:

    def __init__(self, state_score : 'StateScore'):

        self._state_score = state_score
        self._regions = state_score.regions
        self._cur_index = 0

    def __next__(self):

        if self._cur_index < len(self._regions):
            region_name = self._regions[self._cur_index]
            self._cur_index += 1
            return (region_name, self._state_score.score_for_region(region_name))

        raise StopIteration

class Score(ABC):

    """
    An interface for different scores. The scores differ by how the arithmetic
    operations are conducted on them, hence we have to offer a joint interface.
    """

    _Registry = {}

    def __init__(self,
                 score_computer,
                 score_inverter):

        self.score_computer = score_computer
        self.score_inverter = score_inverter
        self.score = 0

    @abstractmethod
    def __add__(self,
                other_score):
        pass

    @abstractmethod
    def __mul__(self,
                scalar):
        pass

    @classmethod
    def register(cls,
                 name : str):

        def decorator(score_class):
            cls._Registry[name] = score_class
            return score_class

        return decorator

    @classmethod
    def get(cls,
            name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use the non-existent score {name}')

        return cls._Registry[name]

    def __lt__(self,
               other_score : 'Score'):

        if not isinstance(other_score, self.__class__):
            raise TypeError(f'Incorrect type of the operand: {other_score.__class__.__name__}. Expected {self.__class__.__name__}')

        if self.score < other_score.score:
            return True
        else:
            return False

    def __le__(self,
               other_score : 'Score'):

        if not isinstance(other_score, self.__class__):
            raise TypeError(f'Incorrect type of the operand: {other_score.__class__.__name__}. Expected {self.__class__.__name__}')

        if self.score <= other_score.score:
            return True
        else:
            return False

    def __gt__(self,
               other_score : 'Score'):

        if not isinstance(other_score, self.__class__):
            raise TypeError(f'Incorrect type of the operand: {other_score.__class__.__name__}. Expected {self.__class__.__name__}')

        if self.score > other_score.score:
            return True
        else:
            return False

    def __ge__(self,
               other_score : 'Score'):

        if not isinstance(other_score, self.__class__):
            raise TypeError(f'Incorrect type of the operand: {other_score.__class__.__name__}. Expected {self.__class__.__name__}')

        if self.score >= other_score.score:
            return True
        else:
            return False

    def __eq__(self,
               other_score : 'Score'):

        if not isinstance(other_score, self.__class__):
            raise TypeError(f'Incorrect type of the operand: {other_score.__class__.__name__}. Expected {self.__class__.__name__}')

        if self.score == other_score.score:
            return True
        else:
            return False

    def __ne__(self,
               other_score : 'Score'):

        if not isinstance(other_score, self.__class__):
            raise TypeError(f'Incorrect type of the operand: {other_score.__class__.__name__}. Expected {self.__class__.__name__}')

        if self.score != other_score.score:
            return True
        else:
            return False

    def get_original_value(self):
        return self.score_inverter(self.score)

    def is_sane(self):

        """
        Checks whether such a score makes sense, i.e. no infinetely appealing
        scores are allowed.
        """

        return (not math.isinf(self.score))

@Score.register('PriceScoreCalculator')
class PriceScore(Score):

    """
    Score based on the price of the option. Since the higher score is better,
    this score is computed by dividing 1 over the price.
    """

    def __init__(self, price_in = float('Inf')):

        price = price_in if isinstance(price_in, numbers.Number) else price_in.value

        super().__init__(InvertingFunction(lambda price: 1 / price),
                         InvertingFunction(lambda score: 1 / score))

        self.score = self.score_computer(price)

    def __add__(self,
                other_score : 'PriceScore'):

        if not isinstance(other_score, self.__class__):
            raise TypeError(f'An attempt to add the score of type {other_score.__class__.__name__} to the score of type {self.__class__.__name__}')

        return self.__class__(self.get_original_value() + other_score.get_original_value())


    def __mul__(self,
                scalar):

        if not isinstance(scalar, numbers.Number):
            raise TypeError(f'An attempt to multiply {self.__class__.__name__} by non-number')

        new_score = self.__class__()
        new_score.score = self.score * scalar
        return new_score

    def __truediv__(self,
                    scalar):

        if not isinstance(scalar, numbers.Number):
            raise TypeError(f'An attempt to divide {self.__class__.__name__} by non-number')

        new_score = self.__class__()
        if scalar == 0:
            new_score.score = float('Inf')
        else:
            new_score.score = self.score / scalar

        return new_score
