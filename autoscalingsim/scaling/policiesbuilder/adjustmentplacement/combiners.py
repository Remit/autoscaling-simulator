from abc import ABC, abstractmethod
import pandas as pd
from collections import OrderedDict

from ....utils.error_check import ErrorChecker

class Combiner(ABC):

    """
    Wraps various strategies to combine adjustment events for the entities on
    a unified timeline.
    """

    @abstractmethod
    def __init__(self,
                 settings : dict):

        pass

    @abstractmethod
    def combine(self,
                scaled_entities_adjustments : dict):

        pass

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
                scaled_entities_adjustments : dict):

        pass

class WindowedCombiner(Combiner):

    """
    Combines adjustments for entities on the unified timeline by slicing their
    individual timelines based on windows and aggregating the adjustments in
    these windows as a cumulative change, e.g. by summing over them. For such
    a combiner to be of use, the windowing parameter should not be set too large.
    """

    def __init__(self,
                 settings : dict):

        window = ErrorChecker.key_check_and_load('window', settings, self.__class__.__name__)
        if not isinstance(window, pd.Timedelta):
            raise TypeError('Parameter \'window\' in {} is not of type {}'.format(self.__class__.__name__, pd.Timedelta.__name__))
        self.window = window
        self.aggregation_operation = sum # todo: consider parameterizing, should return single int

    def combine(self,
                scaled_entities_adjustments : dict,
                cur_timestamp : pd.Timestamp):

        unified_timeline_of_adjustments = {}
        cur_begin = cur_timestamp
        cur_end = cur_begin + self.window
        while len(scaled_entities_adjustments) > 0:

            if not cur_begin in unified_timeline_of_adjustments:
                unified_timeline_of_adjustments[cur_begin] = {}

            for scaled_entity, scaled_entity_adjustment_timeline in scaled_entities_adjustments.items():
                entity_adjustments_in_window = scaled_entity_adjustment_timeline[(scaled_entity_adjustment_timeline.index >= cur_begin) and (scaled_entity_adjustment_timeline.index < cur_end)]
                cumulative_entity_adjustment_in_window = self.aggregation_operation(entity_adjustments_in_window['value'])
                unified_timeline_of_adjustments[cur_begin][scaled_entity] = cumulative_entity_adjustment_in_window
                scaled_entities_adjustments[scaled_entity] = scaled_entity_adjustment_timeline[scaled_entity_adjustment_timeline.index >= cur_end]

            scaled_entities_adjustments = {scaled_entity, scaled_entity_adjustment_timeline for scaled_entity, scaled_entity_adjustment_timeline in scaled_entities_adjustments.items() if len(scaled_entity_adjustment_timeline) > 0}

            cur_begin = cur_end
            cur_end += self.window

        unified_timeline_of_adjustments = OrderedDict(sorted(unified_timeline_of_adjustments.items(),
                                                             key = lambda elem: elem[0]))
        return unified_timeline_of_adjustments

combiners_registry = {
    'noop': NoopCombiner,
    'windowed': WindowedCombiner
}
