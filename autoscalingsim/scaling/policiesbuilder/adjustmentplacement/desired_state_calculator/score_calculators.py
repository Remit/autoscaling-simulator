from abc import ABC, abstractmethod

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
                 container_for_scaled_entities_types : dict):

        self.container_for_scaled_entities_types = container_for_scaled_entities_types

    @abstractmethod
    def __call__(self,
                 container_name : str,
                 duration : pd.Timedelta,
                 containers_count : int) -> tuple:
        pass

class PriceScoreCalculator(ScoreCalculator):

    """
    Implements calculation of the score based on price.
    """

    def __call__(self,
                 container_name : str,
                 duration : pd.Timedelta,
                 containers_count : int) -> tuple:

        if not container_name in self.container_for_scaled_entities_types:
            raise ValueError('Non-existent container type - {}'.format(container_name))
        # TODO: consider taking cpu_credits_h into account
        price = duration * containers_count * self.container_for_scaled_entities_types[container_name].price_p_h
        score = 1 / price

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
            raise ValueError('An attempt to use the non-existent score calculator {}'.format(name))

        return Registry.registry[name]
