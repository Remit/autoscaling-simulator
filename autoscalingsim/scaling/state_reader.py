from autoscalingsim.application.application_structure import ApplicationStructure

class StateReader:

    """
    A read-only access point to the scattered metrics associated with
    different sources such as services, load model, and response stats.
    """

    def __init__(self, sources_dict : dict = None, application_structure : ApplicationStructure = None):

        self.sources = sources_dict if not sources_dict is None else dict()
        self.application_structure = application_structure

    def add_source(self, source_name : str, source_ref):

        if not source_name in self.sources: self.sources[source_name] = source_ref

    def get_aspect_value(self, source_name : str, region_name : str, aspect_name : str):

        return self.sources[source_name].get_aspect_value(region_name, aspect_name)

    def get_metric_value(self, source_name : str, region_name : str, metric_name : str, submetric_name : str):

        return self.sources[source_name].get_metric_value(region_name, metric_name, submetric_name)

    def get_metric_value_for_previous_services(self, service_name : str, region_name : str, metric_name : str, submetric_name : str):

        return self._get_metric_value_for_multiple_sources(self.application_structure.get_prev_services(service_name), region_name, metric_name, submetric_name)

    def get_metric_value_for_next_services(self, service_name : str, region_name : str, metric_name : str, submetric_name : str):

        return self._get_metric_value_for_multiple_sources(self.application_structure.get_next_services(service_name), region_name, metric_name, submetric_name)

    def _get_metric_value_for_multiple_sources(self, source_names : list, region_name : str, metric_name : str, submetric_name : str):

        return { source_name : self.get_metric_value(source_name, region_name, metric_name, submetric_name) for source_name in source_names }

    def get_resource_requirements(self, source_name : str):

        return self.sources[source_name].get_resource_requirements()

    def get_placement_parameter(self, source_name : str, region_name : str, parameter : str):

        return self.sources[source_name].get_placement_parameter(region_name, parameter)
