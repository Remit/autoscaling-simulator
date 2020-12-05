import pandas as pd

from .calc_config import DesiredChangeCalculatorConfig
from .optimizers import Optimizer
from . import score_calculators
from .placer import Placer
from .scorer import Scorer
from .score import StateScore

from autoscalingsim.scaling.state_reader import StateReader
from autoscalingsim.desired_state.region import Region
from autoscalingsim.desired_state.platform_state import PlatformState
from autoscalingsim.desired_state.service_group.group_of_services_reg import GroupOfServicesRegionalized

class DesiredChangeCalculator:

    """ Implements PSO (Place-Score-Optimize) process """

    def __init__(self,
                 scorer : Scorer,
                 scaled_service_instance_requirements_by_service : dict,
                 calc_conf : DesiredChangeCalculatorConfig):

        self.placer = Placer(calc_conf.placement_hint,
                             calc_conf.node_for_scaled_services_types,
                             scaled_service_instance_requirements_by_service,
                             calc_conf.state_reader)

        self.scorer = scorer
        optimizer_class = Optimizer.get(calc_conf.optimizer_type)
        self.optimizer = optimizer_class()

        self.node_for_scaled_services_types = calc_conf.node_for_scaled_services_types
        self.scaled_service_instance_requirements_by_service = scaled_service_instance_requirements_by_service

    def __call__(self, group_of_services_reg : GroupOfServicesRegionalized, state_duration : pd.Timedelta):

        # TODO: add logic to check whether empty results are returned
        regions = {}
        scores_per_region = {}

        for region_name, group_of_services in group_of_services_reg:
            # Place
            placements_lst = self.placer.compute_nodes_requirements(group_of_services, region_name)

            # Score
            scored_placements_lst = self.scorer(placements_lst, state_duration)

            # Optimize
            selected_placement = self.optimizer(scored_placements_lst)

            for ep in selected_placement.services_placements:
                print('ep11')
                print(ep.nodes_count)
                print(ep.services_state.services_counts)

            regions[region_name] = Region.from_conf(region_name, selected_placement)

            scores_per_region[region_name] = selected_placement.score

        # Building the new state based on the selected node type
        desired_state = PlatformState(regions)
        desired_deltas = desired_state.to_deltas()

        return (desired_deltas, StateScore(scores_per_region))
