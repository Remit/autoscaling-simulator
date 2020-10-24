class Sum:

    """
    Produces the sum of the states provided to it.
    """

    @staticmethod
    def aggregate(states_list : list,
                  conf : dict = {}):

        aggregated_val = None
        if len(states_list) > 0:
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
    def aggregate(states_list : list,
                  conf : dict = {}):

        selected_state = None

        last_max_val = 0
        for state in states_list:
            regionalized_repr = state.extract_countable_representation(conf)
            for reg_repr in regionalized_repr.values():
                repr_val = sum(list(reg_repr.values()))
                if repr_val > last_max_val:
                    last_max_val = repr_val
                    selected_state = state

        return selected_state

class Min:

    """
    Select the max state.
    """

    @staticmethod
    def aggregate(states_list : list,
                  conf : dict = {}):

        selected_state = None

        last_min_val = float('Inf')
        for state in states_list:
            regionalized_repr = state.extract_countable_representation(conf)
            for reg_repr in regionalized_repr.values():
                repr_val = sum(list(reg_repr.values()))
                if repr_val < last_min_val:
                    last_min_val = repr_val
                    selected_state = state

        return selected_state

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
    def get(name : str):

        if not name in Registry.registry:
            raise ValueError('An attempt to use a non-existent aggregation for states {}'.format(name))

        return Registry.registry[name]
