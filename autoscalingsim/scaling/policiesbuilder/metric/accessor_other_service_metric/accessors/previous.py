from autoscalingsim.scaling.policiesbuilder.metric.accessor_other_service_metric.accessor import AccessorToOtherService

@AccessorToOtherService.register('previous')
class AccessorToPreviousService(AccessorToOtherService):

    def get_metric_value(self, service_name : str, region_name : str, metric_name : str, submetric_name : str):

        return self.state_reader.get_metric_value_for_previous_services(service_name, region_name, metric_name, submetric_name)
