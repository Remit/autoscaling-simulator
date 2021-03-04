import os
import collections
import networkx as nx
from matplotlib import pyplot as plt

class ApplicationStructure:

    """
    Encapsulates the structural information about the modeled application.
    The structure is represented with an ordered graph.
    """

    direction_key = 'direction'

    def __init__(self):

        self.app_graph = nx.DiGraph()
        self.entry_services = collections.defaultdict(list)

    def get_next_services(self, service_name : str):

        return self._get_services(service_name, 'upstream')

    def get_prev_services(self, service_name : str):

        return self._get_services(service_name, 'downstream')

    def _get_services(self, service_name : str, direction : str):

        services_names_to_return = list()
        for nbr_service_name, edge_attrs in self.app_graph.adj[service_name].items():
            if edge_attrs[self.__class__.direction_key] == direction:
                services_names_to_return.append(nbr_service_name)

        return services_names_to_return

    def add_next_services(self, source_service_name : str, dest_services : list):

        self._add_services(source_service_name, dest_services, 'upstream')

    def add_prev_services(self, source_service_name : str, dest_services : list):

        self._add_services(source_service_name, dest_services, 'downstream')

    def _add_services(self, source_service_name : str, dest_services : list, direction : str):

        if not self.app_graph.has_node(source_service_name):
            self.app_graph.add_node(source_service_name)

        edges_to_add = list()
        for dest_service_name in dest_services:
            edges_to_add.append((source_service_name,
                                dest_service_name,
                                {self.__class__.direction_key: direction}))

        self.app_graph.add_edges_from(edges_to_add)

    def set_entry_services(self, reqs_processing_infos : dict):

        for req_type, rpi in reqs_processing_infos.items():
            self.entry_services[rpi.entry_service].append(req_type)

    def plot_structure_as_graph(self, app_name : str, path_to_store_figure : str):

        plt.figure(1, figsize = (14, 14))
        pos = nx.spring_layout(self.app_graph)
        step = 50
        cur_color_index = 100 + step
        cmap = plt.get_cmap().colors
        for entry_service_name, req_types in self.entry_services.items():
            nx.draw_networkx_nodes(self.app_graph, pos = pos, nodelist = [entry_service_name],
                                   node_color = cmap[cur_color_index % len(cmap)], label = '\n'.join(req_types))
            cur_color_index += step

        ordinary_nodes = [ label for label in self.app_graph.nodes() if not label in self.entry_services ]
        nx.draw_networkx_nodes(self.app_graph, pos = pos, nodelist = ordinary_nodes,
                               node_color = [0.5, 0.5, 0.5])

        nx.draw_networkx_edges(self.app_graph, pos = pos, edgelist = self.app_graph.edges(), width = 2.0)
        description = nx.draw_networkx_labels(self.app_graph, pos = pos)

        for node, t in description.items():
             original_position = t.get_position()
             new_position = (original_position[0], original_position[1] + 0.05)
             t.set_position(new_position)
             t.set_clip_on(False)

        if not os.path.exists(path_to_store_figure):
            os.makedirs(path_to_store_figure)

        plt.legend(loc = 'center left', bbox_to_anchor = (1.05, 0.5), scatterpoints = 1)
        plt.axis('off')
        plt.savefig(os.path.join(path_to_store_figure, app_name + '.png'), bbox_inches = 'tight', pad_inches = 0)
