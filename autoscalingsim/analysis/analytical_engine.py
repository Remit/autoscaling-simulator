import pandas as pd

from .autoscaling_quality.response_times_cdf import ResponseTimesCDF
from .autoscaling_quality.response_times_hist import ResponseTimesHistogram
from .autoscaling_quality.fulfilled_dropped_bars import FulfilledDroppedBarchart
from .autoscaling_quality.utilization_line import UtilizationLineGraph

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

    def __init__(self,
                 simulation_step : pd.Timedelta,
                 figures_dir = None):

        self.simulation_step = simulation_step
        self.figures_dir = figures_dir

    def build_figures(self,
                      simulation : Simulation,
                      results_dirs : str = '',
                      figures_dir = None):
        # TODO: figure settings from config file?
        # TODO: get results from the results_dir, also need to add storing into it

        figures_dir_in_use = self.figures_dir

        if not figures_dir is None:
            figures_dir_in_use = figures_dir

        # Getting the data into the unified representation for processing
        # either from the simulation or from the results_dir
        response_times_regionalized = {}
        load_regionalized = {}
        buffer_times_regionalized = {}
        network_times_regionalized = {}
        desired_node_count_regionalized = {}
        actual_node_count_regionalized = {}
        utilization_per_service = {}
        if not simulation is None:
            load_regionalized = simulation.load_model.get_generated_load()
            response_times_regionalized = simulation.application_model.response_stats.get_response_times_by_request()
            buffer_times_regionalized = simulation.application_model.response_stats.get_buffer_times_by_request()
            network_times_regionalized = simulation.application_model.response_stats.get_network_times_by_request()
            desired_node_count_regionalized = simulation.application_model.desired_node_count
            actual_node_count_regionalized = simulation.application_model.actual_node_count
            utilization_per_service = simulation.application_model.utilization

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

        # Autoscaling behaviour characterization category
        LoadLineGraph.plot(load_regionalized,
                           resolution = pd.Timedelta(1, unit = 's'),
                           figures_dir = figures_dir_in_use)

        GeneratedRequestsByType.plot(load_regionalized,
                                     figures_dir = figures_dir_in_use)

        NodesUsageLineGraph.plot(desired_node_count_regionalized,
                                 actual_node_count_regionalized,
                                 figures_dir = figures_dir_in_use)

        WaitingServiceBuffersHistogram.plot(buffer_times_regionalized,
                                            bins_size_ms = (self.simulation_step.microseconds // 1000),
                                            figures_dir = figures_dir_in_use)

        DistributionRequestsTimesBarchart.plot(response_times_regionalized,
                                               buffer_times_regionalized,
                                               network_times_regionalized,
                                               figures_dir = figures_dir_in_use)
