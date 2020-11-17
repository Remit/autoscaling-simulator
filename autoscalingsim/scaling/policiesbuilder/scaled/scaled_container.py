from abc import ABC, abstractmethod

class ScaledContainer(ABC):

    """
    A representation of a container that holds instances of the scaled entities,
    e.g. a node/virtual machine. A concrete class that is to be used as a reference
    information source for the adjustment has to implement the methods below.
    For instance, the NodeInfo class for the platform model has to implement them
    s.t. the adjustment policy could figure out which number of discrete nodes to
    provide according to the given adjustment goal.
    """

    @abstractmethod
    def get_name(self):
        pass

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
    def entities_require_capacity(self,
                                  requirements_by_entity,
                                  entities_state):
        pass
