import collections

from ...scaling.policiesbuilder.adjustmentplacement.desired_adjustment_calculator.score import StateScore

class StateDuration:

    """
    Wraps durations for state in particular regions.
    """

    @classmethod
    def from_single_value(cls,
                          duration_per_h : float):

        return cls(collections.defaultdict(lambda: duration_per_h))

    def __init__(self,
                 durations_per_region_h : collections.Mapping):

        self.durations_per_region_h = durations_per_region_h

    def __getitem__(self,
                    region_name : str):

        if not isinstance(region_name, str):
            raise TypeError('Unrecognized type of key to extract the duration: {}'.format(type(region_name)))

        return self.durations_per_region_h.get(region_name, 0)

    def __mul__(self,
                state_score : StateScore):

        if not isinstance(state_score, StateScore):
            raise TypeError('An attempt to multiply {} by an unknown object: {}'.format(self.__class__.__name__,
                                                                                        state_score.__class__.__name__))

        scores_per_region = {}
        for region_name, score in state_score.scores_per_region.items():
            if region_name in self.durations_per_region_h:
                scores_per_region[region_name] = score * self.durations_per_region_h[region_name]

        return StateScore(state_score.score_class)
