from abc import ABC, abstractmethod

class ScaledEntityContainer(ABC):

    """
    A representation of a container that holds instances of the scaled entities,
    e.g. a node/virtual machine. A concrete class that is to be used as a reference
    information source for the adjustment has to implement the methods below.
    For instance, the NodeInfo class for the platform model has to implement them
    s.t. the adjustment policy could figure out which number of discrete nodes to
    provide according to the given adjustment goal.
    """

    @abstractmethod
    def get_capacity(self):
        pass

    @abstractmethod
    def get_cost_per_unit_time(self):
        pass

    @abstractmethod
    def get_performance(self):
        pass

    @abstractmethod
    def fits(self,
             requirements_by_entity):
        pass

    @abstractmethod
    def takes_capacity(self,
                       requirements_by_entity):
        pass

class Adjuster(ABC):

    """
    A generic adjuster interface for specific platform adjusters.
    An adjuster belongs to the platform model. The adjustment action is invoked
    with the abstract adjust method that should be implemented in the derived
    specific adjusters.
    """

    placement_hints = [
        'specialized_nodes',
        'balanced_nodes',
        'existing_mixture' # try to use an existig mixture of services on nodes if possible
    ]

    def __init__(self,
                 placement_hint):

        if not placement_hint in Adjuster.placement_hints:
            raise ValueError('Adjustment preference {} currently not supported in {}'.format(placement_hint, self.__class__.__name__))

    @abstractmethod
    def adjust(self):
        pass

class CostMinimizer(Adjuster):

    """
    An adjuster that tries to adjust the platform capacity such that the cost
    is minimized.

    TODO:
        think of general-purpose optimizer that underlies all the adjusters, but is configured a bit differently
        according to the purposes, or is provided with a different optimization function
    """

    def adjust(self,
               desired_scaled_entities_scaling_events,
               container_for_scaled_entities_types,
               scaled_entity_instance_requirements_by_entity):
        pass

class PerformanceMaximizer(Adjuster):
    pass

class UtilizationMaximizer(Adjuster):
    pass


adjusters_registry = {
    'cost_minimization': CostMinimizer,
    'performance_maximization': PerformanceMaximizer,
    'utilization_maximization': UtilizationMaximizer
}
