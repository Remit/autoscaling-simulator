from abc import ABC, abstractmethod

class LoadPatternParser(ABC):

    @staticmethod
    @abstractmethod
    def parse(pattern_conf : dict):

        pass
