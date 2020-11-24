import pandas as pd

from .score_calculators import *
from .score import StateScore

from .....state.state_duration import StateDuration

class Scorer:

    """
    Implements generalized score computation for different appropriate placement
    options. Uses ScoreCalculator to calaculate the score.
    """

    def __init__(self,
                 score_calculator : ScoreCalculator):

        if not isinstance(score_calculator, ScoreCalculator):
            raise TypeError(f'Incorrect type of the score calaculator: {score_calculator.__class__.__name__}')

        self.score_calculator = score_calculator

    def __call__(self,
                 placements_lst : list,
                 state_duration_h : float):

        sane_placements_lst = []
        for placement in placements_lst:
            cumulative_score = self.score_calculator.score_class(0)
            for entities_placement in placement:

                score, _ = self.score_calculator(entities_placement.node_info,
                                                 state_duration_h,
                                                 entities_placement.nodes_count)
                cumulative_score += score

            # In some cases, the score might not get modified by the above loop.
            # For some types of scores, such as price-based, that would mean an
            # infinetely appealing score, which we want to avoid. We do not select
            # such placements.
            if cumulative_score.is_sane():
                placement.score = cumulative_score
                sane_placements_lst.append(placement)

        return sane_placements_lst

    def evaluate_placements(self,
                            placements_per_region : dict,
                            state_duration : StateDuration):

        if not isinstance(state_duration, StateDuration):
            raise TypeError(f'Unexpected type of the state duration: {state_duration.__class__.__name__}')

        cumulative_scores_per_region = {}
        for region_name, region_placement in placements_per_region.items():
            cumulative_score = self.score_calculator.score_class(0)

            sane_placements_lst = self.__call__([region_placement], state_duration[region_name])
            for placement in sane_placements_lst:
                cumulative_score += placement.score

            if cumulative_score.is_sane():
                cumulative_scores_per_region[region_name] = cumulative_score

        return StateScore(cumulative_scores_per_region)
