class Sum:

    """
    Produces the sum of the states provided to it.
    """

    @staticmethod
    def aggregate(states_list):

        aggregated_val = None
        if len(states_list > 0):
            res_class = states_list[0].__class__
            aggregated_val = res_class()

            for state in states_list:
                aggregated_val += state

        return aggregated_val

class Max:

    """
    Select the max state.
    """

    @staticmethod
    def aggregate(states_list):

        if len(states_list > 0):
            res_class = states_list[0].__class__
            selected_state = states_list[0]

            last_max_cnt = 0
            for state in states_list:
                if sum(list(state.extract_node_counts(True).values())) > last_max_cnt:
                    selected_state = state

            return selected_state
        else:
            return None

class Min:

    """
    Select the max state.
    """

    @staticmethod
    def aggregate(states_list):

        if len(states_list > 0):
            res_class = states_list[0].__class__
            selected_state = states_list[0]

            last_min_cnt = float('Inf')
            for state in states_list:
                if sum(list(state.extract_node_counts(True).values())) < last_min_cnt:
                    selected_state = state

            return selected_state
        else:
            return None

class Registry:

    """
    Stores the states aggregation rules classes and organizes access to them.
    """

    registry = {
        'sum': Sum,
        'max': Max,
        'min': Min
    }

    @staticmethod
    def get(name):

        if not name in Registry.registry:
            raise ValueError('An attempt to use a non-existent aggregation for states {}'.format(name))

        return Registry.registry[name]
