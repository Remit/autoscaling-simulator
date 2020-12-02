from autoscalingsim.desired_state.node_group.node_group import NodeGroup, HomogeneousNodeGroup

class NodeGroupDelta:

    """
    Wraps the node group change and the direction of change, i.e.
    addition or subtraction.
    """

    def __init__(self,
                 node_group : HomogeneousNodeGroup,
                 sign : int = 1,
                 in_change : bool = True,
                 virtual : bool = False):

        if not isinstance(node_group, NodeGroup):
            raise TypeError(f'The provided parameter is not of {NodeGroup.__name__} type: {node_group.__class__.__name__}')
        self.node_group = node_group

        if not isinstance(sign, int):
            raise TypeError(f'The provided sign parameters is not of {int.__name__} type: {sign.__class__.__name__}')
        self.sign = sign

        # Signifies whether the delta is just desired (True) or already delayed (False).
        self.in_change = in_change
        # Signifies whether the delta should be considered during the enforcing or not.
        # The aim of 'virtual' property is to keep the connection between the deltas after the enforcement.
        self.virtual = virtual

    def __repr__(self):

        return f'{self.__class__.__name__}( node_group = {self.node_group}, \
                                            sign = {self.sign}, \
                                            in_change = {self.in_change}, \
                                            virtual = {self.virtual})'

    def copy(self):

        return self.__class__(self.node_group, self.sign, self.in_change, self.virtual)

    def enforce(self):

        return self.__class__(self.node_group, self.sign, False)

    def get_provider(self):

        return self.node_group.node_info.get_provider()

    def get_node_type(self):

        return self.node_group.node_info.node_type

    @property
    def is_scale_down(self):

        return self.sign == -1

    @property
    def is_scale_up(self):

        return self.sign == 1

    @property
    def id(self):

        return self.node_group.id
