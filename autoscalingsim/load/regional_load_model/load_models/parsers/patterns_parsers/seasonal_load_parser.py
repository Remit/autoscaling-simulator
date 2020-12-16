import calendar

from .pattern_parser import LoadPatternParser

class SeasonalLoadPatternParser(LoadPatternParser):

    MONTHS_IDS = {month: index for index, month in enumerate(calendar.month_abbr) if month}
    MONTHS_IDS['all'] = 0

    _Registry = {}

    @classmethod
    def register(cls, name : str):

        def decorator(load_pattern_parser_class):
            cls._Registry[name] = load_pattern_parser_class
            return load_pattern_parser_class

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent {cls.__name__} {name}')

        return cls._Registry[name]

from .seasonal import *
