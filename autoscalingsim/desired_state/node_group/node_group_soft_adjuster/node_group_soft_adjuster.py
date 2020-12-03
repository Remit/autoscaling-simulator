from abc import ABC, abstractmethod

class NodeGroupSoftAdjuster(ABC):

    _Registry = {}

    def __init__(self, node_group_ref):

        self.node_group_ref = node_group_ref

    @abstractmethod
    def compute_soft_adjustment(self,
                                unmet_changes : dict,
                                scaled_service_instance_requirements_by_service : dict,
                                node_sys_resource_usage_by_service_sorted : dict) -> tuple:

        pass

    @classmethod
    def register(cls, name : str):

        def decorator(node_group_soft_adjuster_cls):
            cls._Registry[name] = node_group_soft_adjuster_cls
            return node_group_soft_adjuster_cls

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent node group soft adjuster {name}')

        return cls._Registry[name]

    @classmethod
    def available_adjusters(cls):

        return cls._Registry.items()

from .soft_adjusters import *
