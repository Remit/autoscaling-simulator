class ScaledServiceScalingSettings:

    def __init__(self,
                 metric_groups_descriptions : dict,
                 scaling_effect_aggregation_rule_name : str,
                 scaled_service_name : str,
                 scaled_aspect_name : str):

        self._metric_groups_descriptions = metric_groups_descriptions
        self._scaling_effect_aggregation_rule_name = scaling_effect_aggregation_rule_name
        self._scaled_service_name = scaled_service_name
        self._scaled_aspect_name = scaled_aspect_name

    @property
    def metric_groups_descriptions(self):

        return self._metric_groups_descriptions.copy()

    @property
    def scaling_effect_aggregation_rule_name(self):

        return self._scaling_effect_aggregation_rule_name

    @property
    def scaled_service_name(self):

        return self._scaled_service_name

    @property
    def scaled_aspect_name(self):

        return self._scaled_aspect_name
