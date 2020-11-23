import pandas as pd

from .optimizers import Optimizer
from . import score_calculators
from .placer import Placer
from .scorer import Scorer
from .score import StateScore

from .....utils.state.region import Region
from .....utils.state.statemanagers import StateReader
from .....utils.state.platform_state import PlatformState
from .....utils.state.entity_state.entities_states_reg import EntitiesStatesRegionalized

class DesiredChangeCalculator:

    """
    Implements PSO (Place-Score-Optimize) process. Provides the desired state
    of node groups that can further be used to compute deltas.
    """

    def __init__(self,
                 placement_hint : str,
                 scorer : Scorer,
                 optimizer_type : str,
                 node_for_scaled_services_types : dict,
                 scaled_service_instance_requirements_by_service : dict,
                 reader : StateReader):

        self.placer = Placer(placement_hint,
                             node_for_scaled_services_types,
                             scaled_service_instance_requirements_by_service,
                             reader)

        self.scorer = scorer
        optimizer_class = Optimizer.get(optimizer_type)
        self.optimizer = optimizer_class()

        self.node_for_scaled_services_types = node_for_scaled_services_types
        self.scaled_service_instance_requirements_by_service = scaled_service_instance_requirements_by_service

    def __call__(self,
                 services_states : EntitiesStatesRegionalized,
                 state_duration_h : float):

        # TODO: add logic to check whether empty results are returned
        regions = {}
        scores_per_region = {}

        for region_name, services_state in services_states:
            # Place
            placements_lst = self.placer.compute_nodes_requirements(services_state, region_name)

            # Score
            scored_placements_lst = self.scorer(placements_lst, state_duration_h)

            # Optimize
            selected_placement = self.optimizer(scored_placements_lst)

            for ep in selected_placement.services_placements:
                print('ep11')
                print(ep.nodes_count)
                print(ep.services_state.get_services_counts())

            regions[region_name] = Region.from_conf(region_name, selected_placement)

            scores_per_region[region_name] = selected_placement.score

        # Building the new state based on the selected node type
        desired_state = PlatformState(regions)
        desired_deltas = desired_state.to_deltas()

        return (desired_deltas, StateScore(scores_per_region))
