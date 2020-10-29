from abc import ABC, abstractmethod

class Optimizer(ABC):

    """
    An interface class for different optimizers.
    An optimizer attempts to find the best solution given the results of the
    scoring. For instance, it can search for maximal score.
    """

    @abstractmethod
    def __call__(self,
                 scored_options : dict):

        pass

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

class Registry:

    """
    Stores the calculator classes and organizes access to them.
    """

    registry = {
        'OptimizerScoreMaximizer': OptimizerScoreMaximizer
    }

    @staticmethod
    def get(name):

        if not name in Registry.registry:
            raise ValueError(f'An attempt to use the non-existent optimizer {name}')

        return Registry.registry[name]
