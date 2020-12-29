import pandas as pd
from .size import Size
from .error_check import ErrorChecker

class DurationParser:

    @classmethod
    def parse_to_float(self, config : dict):

        val = ErrorChecker.key_check_and_load('value', config)
        unit = ErrorChecker.key_check_and_load('unit', config)

        return pd.Timedelta(val, unit = unit).microseconds / 1000

class MetricUnitsRegistry:

    _Types_registry = {
        'duration': pd.Timedelta,
        'size': Size
    }

    _Parsers_registry = {
        'duration': DurationParser
    }

    @classmethod
    def get(cls, name : str):

        return cls._Types_registry[name] if name in cls._Types_registry else float

    @classmethod
    def get_parser(cls, name : str):

        return cls._Parsers_registry[name] if name in cls._Parsers_registry else float
