from abc import ABC, abstractmethod
import pandas as pd

class ValuesFilter(ABC):

    """
    An interface for the values filter applied on the preprocessing step
    to the raw metrics values.
    """

    @abstractmethod
    def __init__(self,
                 config):
        pass

    @abstractmethod
    def __call__(self,
                 values):
        pass

class DefaultNA(ValuesFilter):

    """
    Substitutes all the NA values for the default value, e.g. 0.
    """

    def __init__(self,
                 config):

        param_key = 'default_value'
        if param_key in config:
            self.default_value = config[param_key]
        else:
            raise ValueError('Not found key {} in the parameters of the {} filter.'.format(param_key, self.__class__.__name__))

    def __call__(self,
                 values):

        return values.fillna(self.default_value)

value_filter_registry = {}
value_filter_registry['defaultNA'] = DefaultNA
