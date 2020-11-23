from .pattern_parser import LoadPatternParser

class ConstantLoadPatternParser(LoadPatternParser):

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
            raise ValueError(f'An attempt to use a non-existent constant load pattern parser {name}')

        return cls._Registry[name]

from .constant import *
