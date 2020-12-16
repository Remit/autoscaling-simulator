from abc import ABC, abstractmethod

class LoadPatternParser(ABC):

    @classmethod
    @abstractmethod
    def parse(cls, pattern_conf : dict):

        pass
