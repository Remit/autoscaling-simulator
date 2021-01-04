import pandas as pd

from .autoscaling_quality.response_times_cdf import ResponseTimesCDF
from .autoscaling_quality.response_times_hist import ResponseTimesHistogram
from .autoscaling_quality.fulfilled_dropped_bars import FulfilledDroppedBarchart
from .autoscaling_quality.utilization_line import UtilizationLineGraph
from .autoscaling_quality.cost_line import CostLineGraph

from .autoscaling_behav.load_line_graph import LoadLineGraph
from .autoscaling_behav.requests_by_type import GeneratedRequestsByType
from .autoscaling_behav.nodes_usage_line_graph import NodesUsageLineGraph
from .autoscaling_behav.waiting_service_buffers_hist import WaitingServiceBuffersHistogram
from .autoscaling_behav.distribution_requests_times_bars import DistributionRequestsTimesBarchart

from autoscalingsim.simulation.simulation import Simulation

class AnalysisFramework:

    """
    Combines the functionality to build the figures based on the simulation
    results. The following figures are supported:

        - Autoscaling quality evaluation category:
            + CDF of the response times, all the request types on the same plot
            + Histogram of the response times, separately for each request type
            + Barchart of fulfilled requests vs dropped, a bar for each request type
            + System resources utilization

        - Autoscaling behaviour characterization category:
            + Line graph (x axis - time) of the generated requests count,
              all the request types on the same plot
            + Barchart of the overall amount of generated requests by type
            + Line graph (x axis - time) of the desired/current node count,
              separately for each node type
            + Histogram of the waiting times in the service buffers,
              separately for each buffer (idea - to locate the bottleneck service)
            + Barchart of the processing vs waiting vs network time for the fulfilled requests,
              a bar for each request type
            > Autoscaling time budget evaluation graphs
    """

    def __init__(self, simulation_step : pd.Timedelta, figures_dir = None):

        self.simulation_step = simulation_step
        self.figures_dir = figures_dir

    def build_figures_for_single_simulation(self, simulation : Simulation = None, results_dirs : str = '', figures_dir = None):
        # TODO: figure settings from config file?
        # TODO: get results from the results_dir, also need to add storing into it

        figures_dir_in_use = self.figures_dir if figures_dir is None else figures_dir

        # Getting the data into the unified representation for processing
        # either from the simulation or from the results_dir
        response_times_regionalized = simulation.application_model.response_stats.get_response_times_by_request() if not simulation is None else dict()
        load_regionalized = simulation.application_model.load_model.get_generated_load() if not simulation is None else dict()
        buffer_times_regionalized = simulation.application_model.response_stats.get_buffer_times_by_request() if not simulation is None else dict()
        network_times_regionalized = simulation.application_model.response_stats.get_network_times_by_request() if not simulation is None else dict()
        desired_node_count_per_provider = simulation.application_model.desired_node_count if not simulation is None else dict()
        actual_node_count_per_provider = simulation.application_model.actual_node_count if not simulation is None else dict()
        utilization_per_service = simulation.application_model.utilization if not simulation is None else dict()
        infrastructure_cost_per_provider = simulation.application_model.infrastructure_cost if not simulation is None else dict()

        # Building figures with the internal functions
        # Autoscaling quality evaluation category
        ResponseTimesCDF.plot(response_times_regionalized,
                              self.simulation_step,
                              figures_dir = figures_dir_in_use)

        ResponseTimesHistogram.plot(response_times_regionalized,
                                    3 * int(self.simulation_step.microseconds / 1000),
                                    figures_dir = figures_dir_in_use)

        FulfilledDroppedBarchart.plot(response_times_regionalized,
                                      load_regionalized,
                                      figures_dir = figures_dir_in_use)

        UtilizationLineGraph.plot(utilization_per_service,
                                  resolution = pd.Timedelta(1, unit = 's'),
                                  figures_dir = figures_dir_in_use)

        CostLineGraph.plot(infrastructure_cost_per_provider,
                           resolution = pd.Timedelta(1, unit = 's'),
                           figures_dir = figures_dir_in_use)

        # Autoscaling behaviour characterization category
        LoadLineGraph.plot(load_regionalized,
                           resolution = pd.Timedelta(1, unit = 's'),
                           figures_dir = figures_dir_in_use)

        GeneratedRequestsByType.plot(load_regionalized,
                                     figures_dir = figures_dir_in_use)

        NodesUsageLineGraph.plot(desired_node_count_per_provider,
                                 actual_node_count_per_provider,
                                 figures_dir = figures_dir_in_use)

        WaitingServiceBuffersHistogram.plot(buffer_times_regionalized,
                                            bins_size_ms = (self.simulation_step.microseconds // 1000),
                                            figures_dir = figures_dir_in_use)

        DistributionRequestsTimesBarchart.plot(response_times_regionalized,
                                               buffer_times_regionalized,
                                               network_times_regionalized,
                                               figures_dir = figures_dir_in_use)

    def build_comparative_figures(self, simulations_by_name : dict, figures_dir : str = None):

        figures_dir_in_use = self.figures_dir if figures_dir is None else figures_dir

        ResponseTimesCDF.comparative_plot(simulations_by_name, self.simulation_step, figures_dir = figures_dir_in_use)
        FulfilledDroppedBarchart.comparative_plot(simulations_by_name, figures_dir = figures_dir_in_use)
        DistributionRequestsTimesBarchart.comparative_plot(simulations_by_name, figures_dir = figures_dir_in_use)
