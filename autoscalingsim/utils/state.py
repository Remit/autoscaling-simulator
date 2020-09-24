from abc import ABC, abstractmethod

class State(ABC):

    @abstractmethod
    def get_val(self,
                attribute_name):
        pass
