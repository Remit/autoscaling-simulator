import pandas as pd
import collections

def convert_to_class(df : pd.DataFrame,
                     conversion_class : type):

    """
    Converts all the data elements of the data frame to the desired custom class
    provided as the second parameter.
    """

    converted_vals_marked = collections.defaultdict(list)
    converted_vals_marked.update((k, []) for k in ([df.index.name] + df.columns.to_list()))
    for ts, row in df.iterrows():
        converted_vals = [conversion_class(row_elem) for row_elem in row]
        for colname, val in zip(df.columns.to_list(), converted_vals):
            converted_vals_marked[colname].append(val)
        converted_vals_marked[df.index.name].append(ts)

    return pd.DataFrame(converted_vals_marked).set_index(df.index.name)
