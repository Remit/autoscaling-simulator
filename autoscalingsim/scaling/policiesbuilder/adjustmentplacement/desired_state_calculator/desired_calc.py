from .placer import Placer
from .scorer import Scorer
import .optimizers

class DesiredStateCalculator:

    """
    Implements PSO (Place-Score-Optimize) process. Provides the desired state
    of container groups that can further be used to compute deltas.
    """

    def __init__(self,
                 placement_hint : str,
                 score_calculator_class : score_calculators.ScoreCalculator,
                 optimizer_type : str):

        self.placer = Placer(placement_hint)
        score_calculator = score_calculator_class(container_for_scaled_entities_types)
        self.scorer = Scorer(score_calculator)
        optimizer_class = optimizers.Registry.get(optimizer_type)
        self.optimizer = optimizer_class()

    def __call__(self,
                 scaled_entity_instance_requirements_by_entity : dict,
                 entities_state : EntitiesState):

        # TODO: add logic to check whether empty results are returned
        # Place
        containers_required = self.placer.compute_containers_requirements(scaled_entity_instance_requirements_by_entity,
                                                                          entities_state)
        # Score
        scored_options = self.scorer(containers_required)

        # Optimize
        container_name, container_params = self.optimizer(scored_options)

        # TODO: consider forming container groups etc, state??
        regions[region_name] = Region(region_name,
                                      container_for_scaled_entities_types[selected_container_name],
                                      selected_containers_count,
                                      selected_placement_entity_representation,
                                      latest_collective_entity_state,
                                      scaled_entity_instance_requirements_by_entity)


    # Building the new state based on the selected container type and
    # entities state
    # TODO: here we deal with the desired state again, therefore we
    # need to take into account that the containers will be 'in-change' here
    latest_state = PlatformState(regions)
    last_ts = interval[1]

        return desired_state # TODO: and score info for comparison
