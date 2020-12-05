from autoscalingsim.scaling.policiesbuilder.adjustmentplacement.adjusters import Adjuster

@Adjuster.register('performance_maximization')
class PerformanceMaximizer(Adjuster):

    def __init__(self,
                 placement_hint = 'sole_instance',
                 combiner_type = 'windowed'):

        pass
