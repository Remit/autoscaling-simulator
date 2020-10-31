import pandas as pd
from abc import ABC, abstractmethod
from collections import OrderedDict

from .error_check import ErrorChecker

class Combiner(ABC):

    """
    Wraps various strategies to combine adjustment events for the entities on
    a unified timeline.
    """

    _Registry = {}

    @abstractmethod
    def __init__(self,
                 settings : dict):

        pass

    @abstractmethod
    def combine(self,
                scaled_entities_adjustments : dict,
                cur_timestamp : pd.Timestamp):

        pass

    @classmethod
    def register(cls,
                 name : str):

        def decorator(combiner_class):
            cls._Registry[name] = combiner_class
            return combiner_class

        return decorator

    @classmethod
    def get(cls,
            name : str):

        if not name in cls._Registry:
            raise ValueError(f'No Combiner of type {name} found')

        return cls._Registry[name]

@Combiner.register('noop')
class NoopCombiner(Combiner):

    """
    Combines adjustments for entities in such a way that they stay separate --
    basically, it selects the smallest possible interval between them, and
    builds a fine-grained unified timeline.
    """

    def __init__(self,
                 settings : dict):

        pass

    def combine(self,
                scaled_entities_adjustments : dict,
                cur_timestamp : pd.Timestamp):

        pass

@Combiner.register('windowed')
class WindowedCombiner(Combiner):

    """
    Combines adjustments for entities on the unified timeline by slicing their
    individual timelines based on windows and aggregating the adjustments in
    these windows as a cumulative change, e.g. by summing over them. For such
    a combiner to be of use, the windowing parameter should not be set too large.
    """

    def __init__(self,
                 settings : dict):

        self.window = pd.Timedelta(ErrorChecker.key_check_and_load('window_size_ms', settings, self.__class__.__name__), unit = 'ms')
        self.aggregation_operation = sum # todo: consider parameterizing, should return single int

    def combine(self,
                scaled_entities_adjustments : dict, # dict of dataframes
                cur_timestamp : pd.Timestamp):

        unified_timeline_of_adjustments = {}
        cur_begin = cur_timestamp
        cur_end = cur_begin + self.window
        cur_scaled_entities_adjustments = scaled_entities_adjustments.copy()
        while len(cur_scaled_entities_adjustments) > 0:

            if not cur_begin in unified_timeline_of_adjustments:
                unified_timeline_of_adjustments[cur_begin] = {}

            for scaled_entity, timeline_df in cur_scaled_entities_adjustments.items():
                entity_adjustments_in_window = timeline_df[(timeline_df.index >= cur_begin) & (timeline_df.index < cur_end)]
                unified_timeline_of_adjustments[cur_begin][scaled_entity] = entity_adjustments_in_window.apply(self.aggregation_operation).to_dict()
                cur_scaled_entities_adjustments[scaled_entity] = timeline_df[timeline_df.index >= cur_end]

            cur_scaled_entities_adjustments = {scaled_entity: scaled_entity_adjustment_timeline for scaled_entity, scaled_entity_adjustment_timeline in cur_scaled_entities_adjustments.items() if len(scaled_entity_adjustment_timeline) > 0 }

            cur_begin = cur_end
            cur_end += self.window

        unified_timeline_of_adjustments = OrderedDict(sorted(unified_timeline_of_adjustments.items(),
                                                             key = lambda elem: elem[0]))

        return unified_timeline_of_adjustments
