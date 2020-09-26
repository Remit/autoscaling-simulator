class PlacementPolicy:

    """
    Wraps functionality of placing the services instances on the nodes that
    were provided during the adjustment phase. Placement Policy aims to achieve
    the specified goal, e.g. performance isolation (smallest count of service
    instances per node), resource utilization maximization (combining in the same
    node as many instance replicas using different resources as possible).
    """

    def __init__(self,
                 placement_goal):

    def place(self):
