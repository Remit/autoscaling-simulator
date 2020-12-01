import pandas as pd
import collections

from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.desired_adjustment_calculator.score import StateScore

class StateDuration:

    """ Durations for state in the regions """

    @classmethod
    def from_single_value(cls, duration : pd.Timedelta):

        return cls(collections.defaultdict(lambda: duration))

    def __init__(self, durations_per_region : collections.Mapping):

        self.durations_per_region = durations_per_region

    def __mul__(self, state_score : StateScore):

        if not isinstance(state_score, StateScore):
            raise TypeError(f'An attempt to multiply {self.__class__.__name__} by an unknown object: {state_score.__class__.__name__}')

        scores_per_region = { region_name : score * self.durations_per_region[region_name].total_seconds() / 3600 \
                                for region_name, score in state_score if region_name in self.durations_per_region }

        return StateScore(scores_per_region)

    def __getitem__(self, region_name : str):

        if not isinstance(region_name, str):
            raise TypeError(f'Unrecognized type of key to extract the duration: {region_name.__class__.__name__}')

        return self.durations_per_region.get(region_name, pd.Timedelta(0, unit = 'ms'))
