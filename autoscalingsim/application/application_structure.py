import networkx as nx

class ApplicationStructure:

    """
    Encapsulates the structural information about the modeled application.
    The structure is represented with an ordered graph.
    """

    direction_key = 'direction'

    def __init__(self):

        self.app_graph = nx.DiGraph()

    def get_next_services(self, service_name : str):

        return self._get_services(service_name, 'upstream')

    def get_prev_services(self, service_name : str):

        return self._get_services(service_name, 'downstream')

    def _get_services(self, service_name : str, direction : str):

        if not service_name in self.app_graph.adj:
            raise ValueError(f'Service {service_name} not found in the application graph')

        services_names_to_return = []
        for nbr_service_name, edge_attrs in self.app_graph.adj[service_name].items():
            if edge_attrs[self.__class__.direction_key] == direction:
                services_names_to_return.append(nbr_service_name)

        return services_names_to_return

    def add_next_services(self, source_service_name : str, dest_services : list):

        self._add_services(source_service_name, dest_services, 'upstream')

    def add_prev_services(self, source_service_name : str, dest_services : list):

        self._add_services(source_service_name, dest_services, 'downstream')

    def _add_services(self,
                      source_service_name : str,
                      dest_services : list,
                      direction : str):

        edges_to_add = []
        for dest_service_name in dest_services:
            edges_to_add.append((source_service_name,
                                dest_service_name,
                                {self.__class__.direction_key: direction}))

        self.app_graph.add_edges_from(edges_to_add)
