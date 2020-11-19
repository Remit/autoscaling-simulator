from abc import ABC, abstractmethod

from ...utils.error_check import ErrorChecker

class Failure(ABC):

    """ Defines a structure to express the behavior of a concrete failure. """

    @abstractmethod
    def to_regional_state_delta(self):
        pass

    def __init__(self, region_name : str,
                 count_of_entities_affected : int):

        self.region_name = region_name
        self.count_of_entities_affected = count_of_entities_affected

class ServiceFailure(Failure):

    """ A kind of failure attributed to a service """

    _Registry = {}

    @classmethod
    def register(cls,
                 name : str):

        def decorator(service_failure_class):
            cls._Registry[name] = service_failure_class
            return service_failure_class

        return decorator

    @classmethod
    def get(cls,
            name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent service failure class {name}')

        return cls._Registry[name]

    def __init__(self,
                 region_name : str,
                 count_of_entities_affected : int,
                 failure_type_conf : dict):

        super().__init__(region_name, count_of_entities_affected)
        self.service_name = ErrorChecker.key_check_and_load('service_name', failure_type_conf)

class NodeGroupFailure(Failure):

    """ A kind of failure attributed to a node group """

    _Registry = {}

    @classmethod
    def register(cls,
                 name : str):

        def decorator(node_group_failure_class):
            cls._Registry[name] = node_group_failure_class
            return node_group_failure_class

        return decorator

    @classmethod
    def get(cls,
            name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent node group failure class {name}')

        return cls._Registry[name]

    def __init__(self,
                 region_name : str,
                 count_of_entities_affected : int,
                 failure_type_conf : dict):

        super().__init__(region_name, count_of_entities_affected)
        self.node_type = ErrorChecker.key_check_and_load('node_type', failure_type_conf)
        self.provider = ErrorChecker.key_check_and_load('provider', failure_type_conf)

from .realizations import *
