import pandas as pd

class Timeline:

    def __init__(self):

        self.timeline = {}

    def append_at_timestamp(self, cur_timestamp : pd.Timestamp, value : float):

        if not cur_timestamp in self.timeline:
            self.timeline[cur_timestamp] = list()
        self.timeline[cur_timestamp].append(value)

    def get_flattened(self):

        return [ val for timestamp, values_lst in self.timeline.items() for val in values_lst ]

    def to_dict(self):

        return self.timeline
