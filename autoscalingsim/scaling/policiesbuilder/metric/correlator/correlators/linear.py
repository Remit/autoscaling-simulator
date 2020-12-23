import pandas as pd

from autoscalingsim.scaling.policiesbuilder.metric.correlator.correlator import Correlator
from autoscalingsim.utils.error_check import ErrorChecker

def linear_correlation(metrics_vals_1 : pd.Series, metrics_vals_2 : pd.Series, lag : int):

    if metrics_vals_1.std() == 0 or metrics_vals_2.std() == 0:
        return None

    return metrics_vals_1.corr(metrics_vals_2.shift(lag).fillna(0))

@Correlator.register('linear')
class LinearCorrelator(Correlator):

    """ Implements lag derivation for metric time series based on Pearson linear correlation coefficient """

    def __init__(self, config : dict):

        super().__init__(config)

        max_time_lag_raw = ErrorChecker.key_check_and_load('max_time_lag', config, self.__class__.__name__)
        max_time_lag_value = ErrorChecker.key_check_and_load('value', max_time_lag_raw, self.__class__.__name__)
        max_time_lag_unit = ErrorChecker.key_check_and_load('unit', max_time_lag_raw, self.__class__.__name__)
        self.max_time_lag = pd.Timedelta(max_time_lag_value, unit = max_time_lag_unit)

    def get_lag(self, associated_service_metric_vals : pd.DataFrame, other_service_metric_vals : pd.DataFrame) -> dict:

        self._update_data(associated_service_metric_vals, other_service_metric_vals)
        min_resolution = self._get_minimal_resolution()
        max_lag = self.max_time_lag // min_resolution
        lags_range = range(-max_lag, max_lag)

        lags_per_service = dict()
        for service_name, metric_vals in self.other_service_metric_vals.items():
            other_service_metric_vals_resampled = metric_vals.resample(min_resolution).mean()
            associated_service_metric_vals_resampled = self.associated_service_metric_vals.resample(min_resolution).mean()

            corr_raw = { lag : linear_correlation(associated_service_metric_vals_resampled['value'], other_service_metric_vals_resampled['value'], lag) for lag in lags_range }
            corr_pruned = { lag : corr for lag, corr in corr_raw.items() if not corr is None}
            linear_correlation_df = pd.DataFrame({'lags': list(corr_pruned.keys()), 'correlation': list(corr_pruned.values())}).set_index('lags')

            if len(linear_correlation_df) > 0:
                lags_per_service[service_name] = linear_correlation_df.idxmax() * min_resolution

        return lags_per_service

    def _get_minimal_resolution(self):

        minimas_to_consider = [pd.Timedelta(1, unit = 's')]

        for service_name, metric_vals in self.other_service_metric_vals.items():
            other_service_metric_vals_min_resolution = min(metric_vals.index.to_series().diff()[1:])
            if not other_service_metric_vals_min_resolution is pd.NaT: minimas_to_consider.append(other_service_metric_vals_min_resolution)

        associated_service_metric_vals_min_resolution = min(self.associated_service_metric_vals.index.to_series().diff()[1:])
        if not associated_service_metric_vals_min_resolution is pd.NaT: minimas_to_consider.append(associated_service_metric_vals_min_resolution)

        return min(minimas_to_consider)
