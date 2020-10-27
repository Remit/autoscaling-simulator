import pandas as pd

from .score_calculators import *

class Scorer:

    """
    Implements generalized score computation for different appropriate placement
    options. Uses ScoreCalculator to calaculate the score.
    """

    def __init__(self,
                 score_calculator : ScoreCalculator):

        if not isinstance(score_calculator, ScoreCalculator):
            raise TypeError('Incorrect type of the score calaculator'.format(score_calculator.__class__.__name__))

        self.score_calculator = score_calculator

    def __call__(self,
                 placements_lst : list,
                 state_duration_h : float):

        sane_placements_lst = []
        for placement in placements_lst:
            cumulative_score = self.score_calculator.score_class(0)
            for entities_placement in placement:

                score, _ = self.score_calculator(entities_placement.container_info,
                                                 state_duration_h,
                                                 entities_placement.containers_count)
                cumulative_score += score

            # In some cases, the score might not get modified by the above loop.
            # For some types of scores, such as price-based, that would mean an
            # infinetely appealing score, which we want to avoid. We do not select
            # such placements.
            if cumulative_score.is_sane():
                placement.score = cumulative_score
                sane_placements_lst.append(placement)

        return sane_placements_lst
