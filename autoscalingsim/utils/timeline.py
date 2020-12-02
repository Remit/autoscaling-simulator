import pandas as pd

class Timeline:

    def __init__(self, timeline : dict = {}):

        self.timeline = timeline.copy()

    def append_at_timestamp(self, cur_timestamp : pd.Timestamp, value : float):

        if not cur_timestamp in self.timeline:
            self.timeline[cur_timestamp] = list()
        self.timeline[cur_timestamp].append(value)

    def get_flattened(self):

        return [ val for values_lst in self.timeline.values() for val in values_lst ]

    def to_dict(self):

        return self.timeline.copy()

    def merge(self, other : 'Timeline'):

        if not isinstance(other, Timeline):
            raise TypeError(f'Unexpected type {other.__class__.__name__}')

        for timestamp, vals_list in other.timeline.items():
            self.timeline[timestamp] = self.timeline.get(timestamp, list()) + vals_list

    @property
    def is_empty(self):

        return len(self.timeline) == 0

    def __repr__(self):

        return f'{self.__class__.__name__}( timeline = {self.timeline})'
