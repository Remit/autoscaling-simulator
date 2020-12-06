from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.desired_adjustment_calculator.optimizing.optimizer import Optimizer

@Optimizer.register('OptimizerScoreMaximizer')
class OptimizerScoreMaximizer(Optimizer):

    def select_best(self, scored_placements : list):

        if len(scored_placements) > 0:
            max_score = max([placement.score for placement in scored_placements])
            return [placement for placement in scored_placements if placement.score == max_score][0]

        else:
            return None
