import igraph
import collections

class AppStructureGenerator:

    # TODO: wrap by the application generator that took the data from the conf files and provides the settings

    """
    Generates the structure of the application based on the Barabasi-Albert model
    with parameters provided in the paper:

    Vladimir Podolskiy, Maria Patrou, Panos Patros, Michael Gerndt, and Kenneth B. Kent. 2020.
    The weakest link: revealing and modeling the architectural patterns of microservice applications.
    In Proceedings of the 30th Annual International Conference on Computer Science and Software Engineering (CASCON '20).
    IBM Corp., USA, 113–122.

    Reference for BA-model implementation:
    https://igraph.org/python/doc/igraph.GraphBase-class.html#Barabasi
    """

    parameter_sets = {
        'tiered_with_single_center_a': { 'power': 0.05, 'zero_appeal': 0.01},
        'single_center_b': { 'power': 0.9, 'zero_appeal': 0.01},
        'tree_with_multiple_centers_c': { 'power': 0.05, 'zero_appeal': 3.25},
        'pipeline_with_multiple_centers_d': { 'power': 0.9, 'zero_appeal': 3.25}
    }

    default_parameter_set = 'tree_with_multiple_centers_c'

    @classmethod
    def generate(cls, services_count, parameter_set_name : str = None,
                 power : float = None, zero_appeal : float = None):

        if parameter_set_name is None and power is None and zero_appeal is None:
            parameter_set_name = cls.default_parameter_set

        graph_settings = dict()
        if not parameter_set_name is None:
            graph_settings = cls.parameter_sets[parameter_set_name]

        else:
            if not power is None:
                graph_settings['power'] = power

            if not zero_appeal is None:
                graph_settings['zero_appeal'] = zero_appeal

        generated_graph = igraph.Graph.Barabasi(services_count, **graph_settings)
        single_direction_request_path = generated_graph.spanning_tree()

        cur_vertices = collections.deque([0])
        next_edges = set([])
        visited = list()
        while len(cur_vertices) > 0:
            vertex_id = cur_vertices.popleft()
            if not vertex_id in visited:
                nb = single_direction_request_path.neighbors(vertex_id)
                next_edges |= set([(vertex_id, destination) for destination in nb if not destination in visited ])
                cur_vertices.extend(nb)
                visited.append(vertex_id)

        return next_edges
