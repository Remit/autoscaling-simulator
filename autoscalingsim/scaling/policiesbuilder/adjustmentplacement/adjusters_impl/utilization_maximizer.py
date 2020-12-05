from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.adjusters import Adjuster

@Adjuster.register('utilization_maximization')
class UtilizationMaximizer(Adjuster):

    def __init__(self,
                 placement_hint = 'balanced',
                 combiner_type = 'windowed'):

        pass
