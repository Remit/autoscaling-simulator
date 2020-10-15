import pandas as pd
from abc import ABC, abstractmethod

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

    @staticmethod
    def config_check(config_raw):
        keys_to_check = ['name', 'config']
        for key in keys_to_check:
            if not key in config_raw:
                raise ValueError('Key {} not found in the configuration for {}'.format(key, __class__.__name__))

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

class Registry:

    """
    Stores the filter classes and organizes access to them.
    """

    registry = {
        'defaultNA': DefaultNA
    }

    @staticmethod
    def get(name):

        if not name in Registry.registry:
            raise ValueError('An attempt to use the non-existent filter {}'.format(name))

        return Registry.registry[name]
