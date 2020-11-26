from abc import ABC, abstractmethod

class MetricSource(ABC):

    """ Determines the interface of an entity serving as a metric source, e.g. a service """

    @abstractmethod
    def get_aspect_value(self, region_name : str, aspect_name : str):

        pass

    @abstractmethod
    def get_metric_value(self, region_name : str, metric_name : str, submetric_name : str):

        pass

    @abstractmethod
    def get_resource_requirements(self, region_name : str):

        pass

    @abstractmethod
    def get_placement_parameter(self, region_name : str, parameter : str):

        pass
