import collections
import pandas as pd

class Timeline:

    def __init__(self, timeline : dict = None):

        self.timeline = collections.defaultdict(list) if timeline is None else timeline

    def append_at_timestamp(self, cur_timestamp : pd.Timestamp, value):

        self.timeline[cur_timestamp].append(value)

    def get_flattened(self):

        return [ val for values_lst in self.timeline.values() for val in values_lst ]

    def to_dict(self):

        return self.timeline.copy()

    def merge(self, other : 'Timeline'):

        for timestamp, vals_list in other.timeline.items():
            self.timeline[timestamp].extend(vals_list)

    def copy(self):

        return self.__class__(self.timeline.copy())

    @property
    def is_empty(self):

        return len(self.timeline) == 0

    @property
    def beginning(self):

        return min(self.timeline.keys()) if len(self.timeline) > 0 else None

    def __repr__(self):

        return f'{self.__class__.__name__}( timeline = {self.timeline})'

class TimelineOfDeltas(Timeline):

    def cut_starting_at(self, cutting_point : pd.Timestamp, cut_enforced : bool = None):

        new_timeline = dict()
        for ts, vals in self.timeline.items():
            if ts <= cutting_point:
                new_timeline[ts] = vals
            else:
                new_timeline[ts] = [val for val in vals if val.is_enforced != cut_enforced]

        self.timeline = collections.defaultdict(list, new_timeline)

    def cut_ending_at(self, cutting_point : pd.Timestamp, cut_enforced : bool = None):

        new_timeline = dict()
        for ts, vals in self.timeline.items():
            if ts >= cutting_point:
                new_timeline[ts] = vals
            else:
                new_timeline[ts] = [val for val in vals if val.is_enforced != cut_enforced]

        self.timeline = collections.defaultdict(list, new_timeline)

    def between_with_beginning_excluded(self, non_inc_begin : pd.Timestamp, inc_end : pd.Timestamp):

        return { ts : vals for ts, vals in self.timeline.items() if (ts > non_inc_begin) and (ts <= inc_end) }

    @property
    def latest_scheduled_platform_enforcement(self):
        latest_scheduled_platform_enforcement = pd.Timestamp(0)

        for timestamp, state_deltas in self.timeline.items():
            for state_delta in state_deltas:
                if state_delta.contains_platform_scale_up:#contains_platform_state_change
                    if timestamp > latest_scheduled_platform_enforcement:
                        latest_scheduled_platform_enforcement = timestamp

        return latest_scheduled_platform_enforcement
