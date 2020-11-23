class ScaledServiceScalingSettings:

    """
    Wraps all the scaling settings relevant for a ScaledService.
    """

    def __init__(self,
                 metrics_descriptions,
                 scaling_effect_aggregation_rule_name,
                 scaled_service_name,
                 scaled_aspect_name):

        self.metrics_descriptions = metrics_descriptions
        self.scaling_effect_aggregation_rule_name = scaling_effect_aggregation_rule_name
        self.scaled_service_name = scaled_service_name
        self.scaled_aspect_name = scaled_aspect_name
