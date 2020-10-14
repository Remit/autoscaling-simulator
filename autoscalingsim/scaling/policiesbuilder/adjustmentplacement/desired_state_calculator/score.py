from abc import ABC, abstractmethod

import numbers

# TODO: move into utils?
class InvertingFunction:

    """
    A helper class that checks whether the call argument is 0 before conducting
    the division.
    """

    def __init__(self,
                 inverting_function):

        self.inverting_function = inverting_function

    def __call__(self,
                 argument):

        if argument == 0:
            return float('Inf')
        else:
            return self.inverting_function(argument)

class Score(ABC):

    """
    An interface for different scores. The scores differ by how the arithmetic
    operations are conducted on them, hence we have to offer a joint interface.
    """

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



    def __lt__(self,
               other_score : Score):

        if not isinstance(other_score, self.__class__):
            raise TypeError('Incorrect type of the operand: {}. Expected {}'.format(other_score.__class__.__name__,
                                                                                    self.__class__.__name__))

        if self.score < other_score.score:
            return True
        else:
            return False

    def __le__(self,
               other_score : Score):

        if not isinstance(other_score, self.__class__):
            raise TypeError('Incorrect type of the operand: {}. Expected {}'.format(other_score.__class__.__name__,
                                                                                    self.__class__.__name__))

        if self.score <= other_score.score:
            return True
        else:
            return False

    def __gt__(self,
               other_score : Score):

        if not isinstance(other_score, self.__class__):
            raise TypeError('Incorrect type of the operand: {}. Expected {}'.format(other_score.__class__.__name__,
                                                                                    self.__class__.__name__))

        if self.score > other_score.score:
            return True
        else:
            return False


    def __ge__(self,
               other_score : Score):

        if not isinstance(other_score, self.__class__):
            raise TypeError('Incorrect type of the operand: {}. Expected {}'.format(other_score.__class__.__name__,
                                                                                    self.__class__.__name__))

        if self.score >= other_score.score:
            return True
        else:
            return False


    def __eq__(self,
               other_score : Score):

        if not isinstance(other_score, self.__class__):
            raise TypeError('Incorrect type of the operand: {}. Expected {}'.format(other_score.__class__.__name__,
                                                                                    self.__class__.__name__))

        if self.score == other_score.score:
            return True
        else:
            return False


    def __ne__(self,
               other_score : Score):

        if not isinstance(other_score, self.__class__):
            raise TypeError('Incorrect type of the operand: {}. Expected {}'.format(other_score.__class__.__name__,
                                                                                    self.__class__.__name__))

        if self.score != other_score.score:
            return True
        else:
            return False

    def get_original_value(self):
        return self.score_inverter(self.score)

class PriceScore(Score):

    """
    Score based on the price of the option. Since the higher score is better,
    this score is computed by dividing 1 over the price.
    """

    def __init__(self,
                 price = float('Inf')):

        super().__init__(InvertingFunction(lambda price: 1 / price),
                         InvertingFunction(lambda score: 1 / score))

        self.score = self.score_computer(price)

    def __add__(self,
                other_score : PriceScore):

        if not isinstance(other_score, self.__class__):
            raise TypeError('An attempt to add the score of type {} to the score of type {}'.format(other_score.__class__.__name__,
                                                                                                    self.__class__.__name__))

        return self.__class__(self.get_original_value() + other_score.get_original_value())


    def __mul__(self,
                scalar):

        if not isinstance(scalar, numbers.Number):
            raise TypeError('An attempt to multiply {} by non-number'.format(self.__class__.__name__))

        new_score = self.__class__()
        new_score.score = self.score * scalar
        return new_score

    def __truediv__(self,
                    scalar):

        if not isinstance(scalar, numbers.Number):
            raise TypeError('An attempt to divide {} by non-number'.format(self.__class__.__name__))

        new_score = self.__class__()
        if scalar == 0:
            new_score.score = float('Inf')
        else:
            new_score.score = self.score / scalar

        return new_score

class Registry:

    """
    Stores the calculator classes and organizes access to them.
    """

    registry = {
        'PriceScoreCalculator': PriceScore
    }

    @staticmethod
    def get(name):

        if not name in Registry.registry:
            raise ValueError('An attempt to use the non-existent score {}'.format(name))

        return Registry.registry[name]
