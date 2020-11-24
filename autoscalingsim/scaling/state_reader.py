class StateReader:

    """
    Acts as a read-only access point to the scattered metrics associated with
    different sources such as services and other information sources,
    maybe external ones.
    """

    def __init__(self, sources_dict = None):

        self.sources = sources_dict if not sources_dict is None else {}

    def add_source(self, source_ref):

        if not source_ref.name in self.sources: self.sources[source_ref.name] = source_ref

    def get_aspect_value(self, source_name : str, region_name : str, aspect_name : str):

        if not source_name in self.sources:
            raise ValueError(f'An attempt to call the source {source_name} that is not in the list of {self.__class__.__name__}')

        return self.sources[source_name].state.get_aspect_value(region_name, aspect_name)

    def get_metric_value(self, source_name : str, region_name : str, metric_name : str):

        if not source_name in self.sources:
            raise ValueError(f'An attempt to call the source {source_name} that is not in the list of {self.__class__.__name__}')

        return self.sources[source_name].state.get_metric_value(region_name, metric_name)

    def get_resource_requirements(self, source_name : str, region_name : str):

        if not source_name in self.sources:
            raise ValueError(f'An attempt to call the source {source_name} that is not in the list of {self.__class__.__name__}')

        return self.sources[source_name].state.get_resource_requirements(region_name)

    def get_placement_parameter(self, source_name : str, region_name : str, parameter : str):

        if not source_name in self.sources:
            raise ValueError(f'An attempt to call the source {source_name} that is not in the list of {self.__class__.__name__}')

        return self.sources[source_name].state.get_placement_parameter(region_name, parameter)
