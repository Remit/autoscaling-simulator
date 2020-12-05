import numbers

from .score import Score

class StateScore:

    def __init__(self, scores_per_region : dict):

        self.scores_per_region = scores_per_region

    def __add__(self, other : 'StateScore'):

        scores_per_region = self.scores_per_region
        for region_name, other_region_score in other.scores_per_region.items():
            if not region_name in scores_per_region:
                scores_per_region[region_name] = other_region_score
            else:
                scores_per_region[region_name] += other_region_score

        return self.__class__(scores_per_region)

    def __mul__(self, state_duration):

        scores_per_region = dict()
        if isinstance(state_duration, StateDuration):
            for region_name, score in self.scores_per_region.items():
                if region_name in state_duration.durations_per_region_h:
                    scores_per_region[region_name] = score * state_duration.durations_per_region_h[region_name]

        elif isinstance(state_duration, numbers.Number):
            for region_name, score in self.scores_per_region.items():
                scores_per_region[region_name] = score * state_duration

        return self.__class__(scores_per_region)

    def __truediv__(self, other : numbers.Number):

        scores_per_region = { region_name : score / other for region_name, score in self.scores_per_region.items() }

        return self.__class__(scores_per_region)

    def score_for_region(self, region_name : str):

        return self.scores_per_region[region_name]

    @property
    def is_worst(self):

        return True if len(self.scores_per_region) == 0 else False

    @property
    def joint_score(self):

        if len(self.scores_per_region) > 0:
            scores = list(self.scores_per_region.values())
            joint_score = sum(scores, scores[0].__class__.build_init_score())
            return scores[0].__class__.build_worst_score() if (not joint_score.is_finite or joint_score is None) else joint_score

        else:
            return Score.create_worst_score()

    @property
    def regions(self):

        return list(self.scores_per_region.keys())

    def __iter__(self):

        return StateScoreIterator(self)

class StateScoreIterator:

    def __init__(self, state_score : 'StateScore'):

        self._state_score = state_score
        self._regions = state_score.regions
        self._cur_index = 0

    def __next__(self):

        if self._cur_index < len(self._regions):
            region_name = self._regions[self._cur_index]
            self._cur_index += 1
            return (region_name, self._state_score.score_for_region(region_name))

        raise StopIteration
