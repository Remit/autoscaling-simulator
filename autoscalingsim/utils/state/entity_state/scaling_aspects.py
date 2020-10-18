from abc import ABC, abstractmethod

class ScalingAspect(ABC):

    """
    An abstract interface for various scaling aspects associated with
    scaled entities.
    """

    def __init__(self,
                 name : str,
                 value : float):

        self.name = name
        self.value = value

    def set_value(self,
                  value : float):

        self.value = value

    @abstractmethod
    def __add__(self,
                other_aspect_val):
        pass

    @abstractmethod
    def __sub__(self,
                other_aspect_val):
        pass

    @abstractmethod
    def __mul__(self,
                other_aspect_val):
        pass

    @abstractmethod
    def __mod__(self,
                other_aspect_val):
        pass

class Count(ScalingAspect):

    """
    Count of scaled entities.
    """

    def __init__(self,
                 value : float):

        if value < 0:
            raise ValueError('Count cannot be negative')

        super().__init__('count',
                         value)

    def __add__(self,
                other_aspect_val : Count):

        if not isinstance(other_aspect_val, Count):
            raise TypeError('An attempt to add an object of unknown type {} to {}'.format(other_aspect_val.__class__.__name__,
                                                                                          self.__class__.__name__))

        return Count(self.value + other_aspect_val.value)

    def __sub__(self,
                other_aspect_val : Count):

        if not isinstance(other_aspect_val, Count):
            raise TypeError('An attempt to subtract an object of unknown type {} from {}'.format(other_aspect_val.__class__.__name__,
                                                                                                 self.__class__.__name__))

        return Count(self.value - other_aspect_val.value)

    def __mul__(self,
                scalar : int):

        if not isinstance(other_aspect_val, int):
            raise TypeError('An attempt to multiply by non-int of type {}'.format(type(scalar)))

        return Count(self.value * scalar)

    def __mod__(self,
                other_aspect_val : Count):

        if not isinstance(other_aspect_val, Count):
            raise TypeError('An attempt to perform modulo operation on {} with an object of unknown type {}'.format(self.__class__.__name__,
                                                                                                                    other_aspect_val.__class__.__name__))

        return Count(self.value % other_aspect_val.value)

class Registry:

    """
    Stores scaling aspects classes and organizes access to them.
    """

    registry = {
        'count': Count
    }

    @staticmethod
    def get(name):

        if not name in Registry.registry:
            raise ValueError('An attempt to use a non-existent scaling aspect {}'.format(name))

        return Registry.registry[name]
