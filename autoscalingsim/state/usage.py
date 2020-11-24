from abc import ABC, abstractmethod

class Usage(ABC):

    """
    Defines a capacity abstraction. This abstraction allows to determine how
    much of a particular resource is taken. In particular, it accumulates
    different types of capacities (e.g. by resource type such as CPU, memory)
    and allows to conduct mathematical operations on them.
    """

    @abstractmethod
    def __add__(self, v_to_add):
        pass

    @abstractmethod
    def __sub__(self, usage_to_sub):
        pass

    @abstractmethod
    def __mul__(self, multiplier):
        pass

    @abstractmethod
    def is_full(self):
        pass

    @abstractmethod
    def is_zero(self):
        pass

    @abstractmethod
    def collapse(self):

        """ Defines how a joint NORMALIZED usage representation is calculated """

        pass
