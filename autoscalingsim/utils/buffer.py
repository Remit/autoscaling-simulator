import collections

class Buffer:

    def __init__(self, capacity : int):

        self._buffer = list()
        self._capacity = capacity

    def put(self, val):

        self._buffer.append(val)
        self._buffer = self._buffer[ -self._capacity : ]

    def get_if_full(self):

        if len(self._buffer) == self._capacity:
            contents = self._buffer
            self._buffer = list()
            return contents
        else:
            return None

class SetOfBuffers:

    def __init__(self, capacity : int):

        self._buffers = collections.defaultdict(lambda: Buffer(capacity))

    def put(self, vals):

        for buffer_name, val in vals.items():
            self._buffers[buffer_name].put(val)

    def get_if_full(self):

        result = collections.defaultdict(list)
        for buf_name, buffer in self._buffers.items():
            buf_res = buffer.get_if_full()
            if buf_res is None:
                return None

            result[buf_name] = buf_res

        return result
