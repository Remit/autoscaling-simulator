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

class Registry:

    """
    Stores the states aggregation rules classes and organizes access to them.
    """

    registry = {
        'sum': Sum
    }

    @staticmethod
    def get(name):

        if not name in Registry.registry:
            raise ValueError('An attempt to use a non-existent aggregation for states {}'.format(name))

        return Registry.registry[name]
