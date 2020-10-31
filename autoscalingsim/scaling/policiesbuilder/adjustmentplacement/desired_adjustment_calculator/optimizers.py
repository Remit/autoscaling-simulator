from abc import ABC, abstractmethod

class Optimizer(ABC):

    """
    An interface class for different optimizers.
    An optimizer attempts to find the best solution given the results of the
    scoring. For instance, it can search for maximal score.
    """

    _Registry = {}

    @abstractmethod
    def __call__(self,
                 scored_options : dict):

        pass

    @classmethod
    def register(cls,
                 name : str):

        def decorator(optimizer_class):
            cls._Registry[name] = optimizer_class
            return optimizer_class

        return decorator

    @classmethod
    def get(cls,
            name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use the non-existent optimizer {name}')

        return cls._Registry[name]

@Optimizer.register('OptimizerScoreMaximizer')
class OptimizerScoreMaximizer(Optimizer):

    def __call__(self,
                 scored_placements_lst : list):

        selected_placement = None

        if len(scored_placements_lst) > 0:
            selected_placement = scored_placements_lst[0]

            for placement in scored_placements_lst[1:]:
                if placement.score > selected_placement.score:
                    selected_placement = placement

        return selected_placement    
