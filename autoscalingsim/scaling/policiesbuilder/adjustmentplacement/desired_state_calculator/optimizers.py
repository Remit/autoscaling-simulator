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
                 scored_options : dict):

        if len(scored_options) > 0:
            selected_container_name = list(scored_options.keys())[0]
            sel_val = list(scored_options.values())[0]

            for container_name, scored_option in scored_options.items():
                if scored_option['score'] > sel_val['score']:
                    selected_container_name = container_name
                    sel_val = scored_option

        return (selected_container_name, sel_val)

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
            raise ValueError('An attempt to use the non-existent optimizer {}'.format(name))

        return Registry.registry[name]
