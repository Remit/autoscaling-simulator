from abc import ABC, abstractmethod

class Capacity(ABC):

    """
    Defines a capacity abstraction. This abstraction allows to determine how
    much of a particular resource is taken. In particular, it accumulates
    different types of capacities (e.g. by resource type such as CPU, memory)
    and allows to conduct mathematical operations on them.

    The capacity is considered in its normalized version, i.e. it has no units.
    Even if the specification has fields that correspond to the particular
    types of resources, it nevertheless should be normalized by the capacity
    of the corresponding container type. For instance, vCPU field in the
    capacity specification can be 0.5 if for the given node type we have
    4 vCPUs, and the service instance takes 2 vCPUs. Such normalization
    allows us to define the capacity algebra to simplify the scaling
    decisions that take capacity into account.
    """

    @abstractmethod
    def __add__(self,
                cap_to_add):
        pass

    @abstractmethod
    def __sub__(self,
                cap_to_sub):
        pass

    @abstractmethod
    def __mul__(self,
                scalar):
        pass

    @abstractmethod
    def is_exhausted(self):
        pass

    @abstractmethod
    def is_empty(self):
        pass

    @abstractmethod
    def collapse(self):
        """
        Defines how a joint capacity representation is calculated. For instance,
        in case of system capacity one can simply sum over all the individual
        capacities and divide by their count.
        """
        pass
