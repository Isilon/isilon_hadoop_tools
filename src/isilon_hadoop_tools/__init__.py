"""Isilon Hadoop Tools"""


from pkg_resources import get_distribution


__all__ = [
    # Constants
    '__version__',
    # Exceptions
    'IsilonHadoopToolError',
]
__version__ = get_distribution(__name__).version


class IsilonHadoopToolError(Exception):
    """All Exceptions emitted from this package inherit from this Exception."""

    def __str__(self):
        return super(IsilonHadoopToolError, self).__str__() or repr(self)

    def __repr__(self):
        return '{0}{cause}'.format(
            super(IsilonHadoopToolError, self).__repr__(),
            cause=(
                ' caused by {0!r}'.format(self.__cause__)  # pylint: disable=no-member
                if getattr(self, '__cause__', None) else
                ''
            ),
        )
