import numbers

from autoscalingsim.utils.functions import InvertingFunction
from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.desired_adjustment_calculator.scoring.score.score import Score

@Score.register('PriceScoreCalculator')
class PriceScore(Score):

    def __init__(self, price_in : float = float('Inf')):

        price = price_in if isinstance(price_in, numbers.Number) else price_in.value

        super().__init__(InvertingFunction(lambda price: 1 / price),
                         InvertingFunction(lambda score: 1 / score))

        self.score = self.score_computer(price)

    def __add__(self, other : 'PriceScore'):

        return self.__class__(self.original_value + other.original_value)

    def __mul__(self, other : numbers.Number):

        new_score = self.__class__()
        new_score.score = self.score * other

        return new_score

    def __truediv__(self, other : numbers.Number):

        new_score = self.__class__()
        new_score.score = float('Inf') if other == 0 else self.score / other

        return new_score

    @classmethod
    def build_init_score(cls):

        return cls(0)

    @classmethod
    def build_worst_score(cls):

        return cls(float('Inf'))
