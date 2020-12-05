import pandas as pd

from .score_calculator import ScoreCalculator
from .score import StateScore

from autoscalingsim.desired_state.platform_state import PlatformState
from autoscalingsim.desired_state.state_duration import StateDuration

class Scorer:

    def __init__(self, score_calculator : ScoreCalculator):

        self.score_calculator = score_calculator

    def score_placements(self, placements_lst : list, state_duration : pd.Timedelta):

        allowed_placements = list()
        for placement in placements_lst:
            cumulative_score = self.score_calculator.build_init_score()
            for entities_placement in placement:
                score, _ = self.score_calculator.compute_score(entities_placement.node_info, state_duration, entities_placement.nodes_count)
                cumulative_score += score

            if cumulative_score.is_finite:
                placement.score = cumulative_score
                allowed_placements.append(placement)

        return allowed_placements

    def score_platform_state(self, platform_state : PlatformState, state_duration : StateDuration):

        cumulative_scores_per_region = dict()
        for region_name, region_placement in platform_state.to_placements().items():
            cumulative_score = self.score_calculator.build_init_score()

            allowed_placements = self.score_placements([region_placement], state_duration[region_name])
            for placement in allowed_placements:
                cumulative_score += placement.score

            if cumulative_score.is_finite:
                cumulative_scores_per_region[region_name] = cumulative_score

        return StateScore(cumulative_scores_per_region)
