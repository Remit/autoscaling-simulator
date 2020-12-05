from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.scaling.scaling_model import ScalingModel
from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.adjusters import Adjuster
from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.desired_adjustment_calculator.score_calculators import ScoreCalculator

@Adjuster.register('cost_minimization')
class CostMinimizer(Adjuster):

    def __init__(self,
                 adjustment_horizon : dict,
                 scaling_model : ScalingModel,
                 node_for_scaled_services_types : dict,
                 scaled_service_instance_requirements_by_service : dict,
                 state_reader : StateReader,
                 optimizer_type = 'OptimizerScoreMaximizer',
                 placement_hint = 'shared',
                 combiner_type = 'windowed'):

        score_calculator_class = ScoreCalculator.get(self.__class__.__name__)
        super().__init__(adjustment_horizon,
                         scaling_model,
                         node_for_scaled_services_types,
                         scaled_service_instance_requirements_by_service,
                         optimizer_type,
                         placement_hint,
                         combiner_type,
                         score_calculator_class,
                         state_reader)
