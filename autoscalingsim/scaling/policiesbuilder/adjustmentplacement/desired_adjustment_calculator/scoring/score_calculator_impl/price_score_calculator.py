import pandas as pd

from autoscalingsim.infrastructure_platform.node_information.node import NodeInfo
from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.desired_adjustment_calculator.scoring.score import Score
from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.desired_adjustment_calculator.scoring.score_calculator import ScoreCalculator

@ScoreCalculator.register('CostMinimizer')
class PriceScoreCalculator(ScoreCalculator):

    """
    Implements calculation of the score based on price.
    """

    def __init__(self):

        super().__init__(Score.get(self.__class__.__name__))

    def compute_score(self, node_info : NodeInfo, duration : pd.Timedelta, nodes_count : int) -> tuple:

        # TODO: consider taking cpu_credits_per_unit_time into account
        price = duration * node_info.price_per_unit_time * nodes_count
        score = self.score_class(price)

        return (score, price)
