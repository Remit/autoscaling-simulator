from copy import deepcopy

class NodeGroupDelta:

    def __init__(self, node_group : 'NodeGroup', sign : int = 1, virtual : bool = False):

        self.node_group = node_group.produce_virtual_copy() if (virtual and not node_group.virtual) else node_group
        self._sign = sign

    def enforce(self):

        return self.__class__(self.node_group.enforce(), self._sign, False)

    def to_virtual(self):

        return self.__class__(self.node_group, self._sign, True)

    def copy(self):

        return self.__class__(self.node_group, self._sign, self.virtual)

    def __deepcopy__(self, memo):

        result = self.__class__(deepcopy(self.node_group, memo), self._sign, self.virtual)
        memo[id(result)] = result
        return result

    @property
    def in_change(self):

        return not self.node_group.enforced

    @property
    def virtual(self):

        return self.node_group.virtual

    @property
    def provider(self):

        return self.node_group.node_info.provider

    @property
    def node_type(self):

        return self.node_group.node_info.node_type

    @property
    def is_scale_down(self):

        return self._sign == -1

    @property
    def is_scale_up(self):

        return self._sign == 1

    @property
    def id(self):

        return self.node_group.id

    @property
    def is_empty(self):

        return self.node_group.nodes_count == 0

    def __repr__(self):

        return f'{self.__class__.__name__}( node_group = {self.node_group}, \
                                            sign = {self._sign}, \
                                            virtual = {self.virtual})'
