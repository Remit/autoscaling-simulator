class ScaledEntityScalingSettings:

    """
    Wraps all the scaling settings relevant for a ScaledEntity.
    """

    def __init__(self,
                 metrics_descriptions,
                 scaling_effect_aggregation_rule_name,
                 scaled_entity_name,
                 scaled_aspect_name):

        self.metrics_descriptions = metrics_descriptions
        self.scaling_effect_aggregation_rule_name = scaling_effect_aggregation_rule_name
        self.scaled_entity_name = scaled_entity_name
        self.scaled_aspect_name = scaled_aspect_name
