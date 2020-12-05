from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.scaling.scaling_model import ScalingModel
from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.adjuster import Adjuster
from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.desired_adjustment_calculator.scoring.score_calculator import ScoreCalculator

@Adjuster.register('cost_minimization')
class CostMinimizer(Adjuster):

    def __init__(self,
                 adjustment_horizon : dict,
                 scaling_model : ScalingModel,
                 scaled_service_instance_requirements_by_service : dict,
                 combiner_settings : dict,
                 calc_conf : 'DesiredChangeCalculatorConfig'):

        score_calculator_class = ScoreCalculator.get(self.__class__.__name__)
        super().__init__(adjustment_horizon,
                         scaling_model,
                         scaled_service_instance_requirements_by_service,
                         combiner_settings,
                         calc_conf,
                         score_calculator_class)
