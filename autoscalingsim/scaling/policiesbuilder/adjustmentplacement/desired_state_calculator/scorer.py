import pandas as pd

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

        for placement in placements_lst:
            cumulative_score = self.get_null_score()
            for entities_placement in placement:

                score, _ = self.score_calculator(entities_placement.container_name,
                                                 state_duration_h,
                                                 entities_placement.containers_count)
                cumulative_score += score

            placement.score = score

        return placements_lst

    def get_null_score(self):

        return self.score_calculator.score_class()
