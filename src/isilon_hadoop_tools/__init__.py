"""Isilon Hadoop Tools"""


from pkg_resources import get_distribution


__all__ = [
    # Constants
    "__version__",
    # Exceptions
    "IsilonHadoopToolError",
]
__version__ = get_distribution(__name__).version


class IsilonHadoopToolError(Exception):
    """All Exceptions emitted from this package inherit from this Exception."""

    def __str__(self):
        return super().__str__() or repr(self)

    def __repr__(self):
        cause = (
            f" caused by {self.__cause__!r}"  # pylint: disable=no-member
            if getattr(self, "__cause__", None)
            else ""
        )
        return f"{super()!r}{cause}"
