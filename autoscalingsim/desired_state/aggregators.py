from abc import ABC, abstractmethod

class StatesAggregator(ABC):

    _Registry = {}

    @classmethod
    def register(cls, name : str):

        def decorator(states_aggregator_cls):
            cls._Registry[name] = states_aggregator_cls
            return states_aggregator_cls

        return decorator

    @classmethod
    def get(cls, name : str):

        if not name in cls._Registry:
            raise ValueError(f'An attempt to use a non-existent aggregator class {name}')

        return cls._Registry[name]

    @abstractmethod
    def aggregate(states_list : list, conf : dict = None):

        pass

@StatesAggregator.register('sum')
class SumStatesAggregator(StatesAggregator):

    """ Sums the states provided to it """

    def aggregate(self, states_list : list, conf : dict = None):

        aggregated_val = None
        if len(states_list) > 0:
            aggregated_val = states_list[0].__class__()

            for state in states_list:
                aggregated_val += state

        return aggregated_val

@StatesAggregator.register('max')
class MaxStatesAggregator(StatesAggregator):

    """ Selects the state with the highest countable representation """

    def aggregate(self, states_list : list, conf : dict = None):

        selected_state = None
        last_max_val = 0

        for state in states_list:
            config_for_counting = {} if conf is None else conf
            regionalized_repr = state.countable_representation(config_for_counting)

            for reg_repr in regionalized_repr.values():
                repr_val = sum(list(reg_repr.values()))
                if repr_val > last_max_val:
                    last_max_val = repr_val
                    selected_state = state

        return selected_state

@StatesAggregator.register('min')
class MinStatesAggregator(StatesAggregator):

    """ Selects the state with the smallest countable representation """

    def aggregate(self, states_list : list, conf : dict = {}):

        selected_state = None
        last_min_val = float('Inf')

        for state in states_list:
            config_for_counting = {} if conf is None else conf
            regionalized_repr = state.countable_representation(config_for_counting)

            for reg_repr in regionalized_repr.values():
                repr_val = sum(list(reg_repr.values()))
                if repr_val < last_min_val:
                    last_min_val = repr_val
                    selected_state = state

        return selected_state
