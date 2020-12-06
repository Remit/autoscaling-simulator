import collections
import pandas as pd

from .calc_config import DesiredPlatformAdjustmentCalculatorConfig
from .optimizing import Optimizer
from .placing import Placer
from .scoring import Scorer, Score, StateScore

from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.desired_state.region import Region
from autoscalingsim.desired_state.platform_state import PlatformState
from autoscalingsim.desired_state.service_group.group_of_services_reg import GroupOfServicesRegionalized

class DesiredPlatformAdjustmentCalculator:

    """ Implements PSO (Place-Score-Optimize) process """

    def __init__(self, scorer : Scorer,
                 services_resource_requirements : dict,
                 calc_conf : DesiredPlatformAdjustmentCalculatorConfig):

        self.placer = Placer(calc_conf.placement_hint,
                             calc_conf.node_for_scaled_services_types,
                             services_resource_requirements,
                             calc_conf.state_reader)

        self.scorer = scorer
        self.optimizer = Optimizer.get(calc_conf.optimizer_type)()

    def compute_adjustment(self, group_of_services_reg : GroupOfServicesRegionalized,
                           state_duration : pd.Timedelta):

        regions = collections.defaultdict(Region)
        scores_per_region = collections.defaultdict(Score)

        for region_name, group_of_services in group_of_services_reg:

            placements = self.placer.compute_nodes_requirements(group_of_services, region_name)
            scored_placements = self.scorer.score_placements(placements, state_duration)
            optimal_placement = self.optimizer.select_best(scored_placements)

            regions[region_name] = Region.from_conf(region_name, optimal_placement)
            scores_per_region[region_name] = optimal_placement.score

        return (PlatformState(regions).to_deltas(), StateScore(scores_per_region))
