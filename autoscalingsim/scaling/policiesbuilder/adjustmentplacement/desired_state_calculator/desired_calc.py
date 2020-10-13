import pandas as pd

from .placer import Placer
from .scorer import Scorer
import .optimizers

# TODO: consider moving region and platform state to utils.
from ......infrastructure_platform.region import Region
from ......infrastructure_platform.entity_group import EntitiesStatesRegionalized
from ......infrastructure_platform.platform_state import PlatformState

class DesiredStateCalculator:

    """
    Implements PSO (Place-Score-Optimize) process. Provides the desired state
    of container groups that can further be used to compute deltas.
    """

    def __init__(self,
                 placement_hint : str,
                 score_calculator_class : score_calculators.ScoreCalculator,
                 optimizer_type : str,
                 container_for_scaled_entities_types : dict,
                 scaled_entity_instance_requirements_by_entity : dict):

        self.placer = Placer(placement_hint)
        score_calculator = score_calculator_class(container_for_scaled_entities_types)
        self.scorer = Scorer(score_calculator)
        optimizer_class = optimizers.Registry.get(optimizer_type)
        self.optimizer = optimizer_class()

        self.container_for_scaled_entities_types = container_for_scaled_entities_types
        self.scaled_entity_instance_requirements_by_entity = scaled_entity_instance_requirements_by_entity

    def __call__(self,
                 entities_states : EntitiesStatesRegionalized,
                 state_duration_h : float):

        # TODO: add logic to check whether empty results are returned
        regions = {}
        joint_score = 0
        for region_name, entities_state in entities_states:
            # Place
            containers_required = self.placer.compute_containers_requirements(self.scaled_entity_instance_requirements_by_entity,
                                                                              entities_state)
            # Score
            scored_options = self.scorer(containers_required, state_duration_h)

            # Optimize
            container_name, container_params = self.optimizer(scored_options)

            regions[region_name] = Region(region_name,
                                          self.container_for_scaled_entities_types[container_name],
                                          container_params['count'],
                                          container_params['placement'],
                                          EntitiesState(),
                                          self.scaled_entity_instance_requirements_by_entity)


            # Building the new state based on the selected container type
            desired_state = PlatformState(regions)
            joint_score += container_params['score']

        return (desired_state, joint_score)
