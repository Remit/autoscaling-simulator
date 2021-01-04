from autoscalingsim.infrastructure_platform.node_information.system_resource_usage import SystemResourceUsage
from cruncher.experimental_regime.experimental_regime import ExperimentalRegime

PUBLISHING_DPI = 600
MAX_PLOTS_CNT_ROW = 4
filename_format = '{}-{}'
SYSTEM_RESOURCES_CNT = len(SystemResourceUsage.system_resources)

def convert_name_of_considered_alternative_to_label(original_string):

    s = '['
    ss = original_string.split(ExperimentalRegime._policies_categories_delimiter)[1:]
    for policy_raw in ss[:-1]:
        k = policy_raw.split(ExperimentalRegime._concretization_delimiter)
        s += f'{k[0]} -> {k[1]}; '

    k = ss[-1].split(ExperimentalRegime._concretization_delimiter)
    s += f'{k[0]} -> {k[1]}]'

    return s
