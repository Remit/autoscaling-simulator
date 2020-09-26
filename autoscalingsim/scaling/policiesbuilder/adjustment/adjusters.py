from abc import ABC, abstractmethod

class Adjuster(ABC):

    """
    A generic adjuster interface for specific platform adjusters.
    An adjuster belongs to the platform model. The adjustment action is invoked
    with the abstract adjust method that should be implemented in the derived
    specific adjusters.
    """

    class ScaledEntityContainer(ABC):

        """
        A representation of a container that holds instances of the scaled entities,
        e.g. a node/virtual machine. A concrete class that is to be used as a reference
        information sourde for the adjustment has to implement th methods below.
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

    # TODO: think of a generalizable node description that
    # can be articulated with the names below
    # TODO: some standard representation of the nodes like capacity containers to fill?
    adjustment_preferences = [
        'specialized_nodes',
        'balanced_nodes'
    ]

    def __init__(self,
                 adjustment_preference):

        if not adjustment_preference in Adjuster.adjustment_preferences:
            raise ValueError('Adjustment preference {} currently not supported in {}'.(adjustment_preference, self.__class__.__name__))

    @abstractmethod
    def adjust(self):
        pass

class CostMinimizer(Adjuster):

    """
    An adjuster that tries to adjust the platform capacity such that the cost
    is minimized.
    """

    def adjust(self,
               desired_scaled_entities_scaling_events,
               container_for_scaled_entities_types,
               scaled_entity_instance_requirements_by_entity):
        pass


adjusters_registry = {
    'cost_minimization': CostMinimizer
}
