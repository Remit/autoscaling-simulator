import operator

class Size:

    sizes_bytes = {
        'B'  : 1,
        'KB' : 1024,
        'MB' : 1048576,
        'GB' : 1073741824,
        'TB' : 1099511627776,
        'PB' : 1125899906842624,
        'EB' : 1152921504606846976,
        'ZB' : 1180591620717411303424 }

    def __init__(self, value : float = 0.0, unit : str = 'B'):

        if not unit in self.__class__.sizes_bytes and not unit.upper() in self.__class__.sizes_bytes:
            raise ValueError(f'Unknown unit {unit}')

        if value < 0: value = 0

        normalizer = 1 if unit in self.__class__.sizes_bytes else 8 # handles the case of *bit unit
        self._size_in_bytes = (value / normalizer) * self.__class__.sizes_bytes[unit.upper()]

    def __add__(self, other : 'Size'):

        return self._add(other, 1)

    def __sub__(self, other : 'Size'):

        return self._add(other, -1)

    def __mul__(self, multiplier : int):

        if not isinstance(multiplier, int):
            raise ValueError(f'Non-int multiplier for {self.__class__.__name___}')

        return self.__class__(self._size_in_bytes * multiplier)

    def __rmul__(self, multiplier : int):

        return self.__mul__(multiplier)

    def __truediv__(self, other : 'Size'):

        return self._div(other, operator.truediv)

    def __floordiv__(self, other : 'Size'):

        return self._div(other, operator.floordiv)

    def __gt__(self, other_size : 'Size'):

        return self._comp(other_size, operator.gt)

    def __ge__(self, other_size : 'Size'):

        return self._comp(other_size, operator.ge)

    def __lt__(self, other_size : 'Size'):

        return self._comp(other_size, operator.lt)

    def __le__(self, other_size : 'Size'):

        return self._comp(other_size, operator.le)

    def __eq__(self, other_size : 'Size'):

        return self._comp(other_size, operator.eq)

    def __ne__(self, other_size : 'Size'):

        return self._comp(other_size, operator.ne)

    def to_bytes(self): return self._to_unit('B')

    def to_kilobytes(self): return self._to_unit('kB')

    def to_megabytes(self): return self._to_unit('MB')

    def to_gigabytes(self): return self._to_unit('GB')

    def to_terabytes(self): return self._to_unit('TB')

    def to_petabytes(self): return self._to_unit('PB')

    def to_exabytes(self): return self._to_unit('EB')

    def to_zetabytes(self): return self._to_unit('ZB')

    def __repr__(self):

        return f'{self.__class__.__name__}({self._size_in_bytes})'

    def _add(self, other : 'Size', sign : int):

        if not isinstance(other, Size):
            raise TypeError(f'Cannot combine object of type {self.__class__.__name__} with object of type {other.__class__.__name__}')

        return self.__class__(self._size_in_bytes + sign * other._size_in_bytes)

    def _to_unit(self, unit : str) -> float:

        return self._size_in_bytes / self.__class__.sizes_bytes[unit]

    def _div(self, other : 'Size', op):

        if not isinstance(other, Size):
            raise ValueError(f'Cannot divide by the value of an unrecognized type {other.__class__.__name__}')

        if other._size_in_bytes == 0:
            raise ValueError('An attempt to divide by zero-size')

        return op(self._size_in_bytes, other._size_in_bytes)

    def _comp(self, other_size : 'Size', comparison_op) -> bool:

        """ Implements common comparison logic """

        if not isinstance(other_size, Size):
            raise TypeError(f'Unexpected type of an operand when comparing with {self.__class__.__name__}: {other_size.__class__.__name__}')

        return comparison_op(self._size_in_bytes, other_size._size_in_bytes)
