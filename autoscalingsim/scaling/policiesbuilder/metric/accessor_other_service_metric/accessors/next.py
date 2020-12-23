from autoscalingsim.scaling.policiesbuilder.metric.accessor_other_service_metric.accessor import AccessorToOtherService

@AccessorToOtherService.register('next')
class AccessorToNextService(AccessorToOtherService):

    def get_metric_value(self, region_name : str):

        return self.state_reader.get_metric_value_for_next_services(self.service_name, region_name, self.metric_name, self.submetric_name)
