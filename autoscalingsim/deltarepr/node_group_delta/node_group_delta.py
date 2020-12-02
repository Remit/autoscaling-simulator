from autoscalingsim.desired_state.node_group.node_group import NodeGroup, HomogeneousNodeGroup

class NodeGroupDelta:

    def __init__(self, node_group : NodeGroup, sign : int = 1,
                 in_change : bool = True, virtual : bool = False):

        self.node_group = node_group
        self.sign = sign
        self.in_change = in_change
        self.virtual = virtual

    def enforce(self):

        return self.__class__(self.node_group, self.sign, False, False)

    def copy(self):

        return self.__class__(self.node_group, self.sign, self.in_change, self.virtual)

    @property
    def provider(self):

        return self.node_group.node_info.get_provider()

    @property
    def node_type(self):

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

    def __repr__(self):

        return f'{self.__class__.__name__}( node_group = {self.node_group}, \
                                            sign = {self.sign}, \
                                            in_change = {self.in_change}, \
                                            virtual = {self.virtual})'
