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

    def cut_starting_at(self, cutting_point : pd.Timestamp):

        self.timeline = collections.defaultdict(list, { ts : vals for ts, vals in self.timeline.items() if ts <= cutting_point })

    def cut_ending_at(self, cutting_point : pd.Timestamp):

        self.timeline = collections.defaultdict(list, { ts : vals for ts, vals in self.timeline.items() if ts >= cutting_point })

    def between_with_beginning_excluded(self, non_inc_begin : pd.Timestamp, inc_end : pd.Timestamp):

        return { ts : vals for ts, vals in self.timeline.items() if (ts > non_inc_begin) and (ts <= inc_end) }

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
