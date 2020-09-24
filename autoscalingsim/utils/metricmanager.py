from .state import State

class MetricManager:

    """
    Acts as an access point to the scattered metrics/values associated with
    different entities such as services and other information sources,
    maybe external ones.
    """

    def __init__(self,
                 sources_dict = None):

        self.sources = {}
        if not sources_dict is None:
            self.sources = sources_dict

    def add_source(self,
                   source_name,
                   source_ref):

        if not source_name in self.sources:
            self.sources[source_name] = source.ref

    def get_values(self,
                   source_name,
                   attribute_name):

        if not source_name in self.sources:
            raise ValueError('An attempt to call the source {} that is not in the list of the {}'.format(source_name, self.__class__.__name__))

        return self.sources[source_name].state.get_val(attribute)
