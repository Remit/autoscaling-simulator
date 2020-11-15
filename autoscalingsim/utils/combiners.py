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
                scaled_entities_adjustments : dict):

        unified_timeline_of_adjustments = {}
        for scaled_entity, timeline_df in scaled_entities_adjustments.items():
            aggregated_timeline = timeline_df.resample(self.window).apply(self.aggregation_operation).to_dict()
            for aspect_name, timed_change in aggregated_timeline.items():
                for ts, change_val in timed_change.items():
                    unified_timeline_of_adjustments[ts] = {scaled_entity : {aspect_name : change_val}}

        unified_timeline_of_adjustments = OrderedDict(sorted(unified_timeline_of_adjustments.items(),
                                                             key = lambda elem: elem[0]))

        return unified_timeline_of_adjustments
