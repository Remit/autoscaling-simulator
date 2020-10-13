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
                 containers_required : dict,
                 state_duration_h : float):

        scored_options = {}
        for container_name, container_count_and_placement in containers_required.items():
            score, parameter_val = self.score_calculator(container_name,
                                                         state_duration_h,
                                                         containers_count_and_placement['count'])
            interval_score = { 'score': score,
                               'parameter': parameter_val,
                               'count': containers_count_and_placement['count'],
                               'placement': containers_count_and_placement['placement_entity_representation']}
            scored_options[container_name] = interval_score

        return scored_options
