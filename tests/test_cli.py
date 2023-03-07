"""Verify the functionality of isilon_hadoop_tools.cli."""


from unittest.mock import Mock

import pytest

from isilon_hadoop_tools import IsilonHadoopToolError, cli


def test_catches(exception):
    """Ensure cli.catches detects the desired exception."""
    assert cli.catches(exception)(Mock(side_effect=exception))() == 1


def test_not_catches(exception):
    """Ensure cli.catches does not catch undesirable exceptions."""
    with pytest.raises(exception):
        cli.catches(())(Mock(side_effect=exception))()


@pytest.mark.parametrize(
    "error, classinfo",
    [
        (cli.CLIError, IsilonHadoopToolError),
        (cli.HintedError, cli.CLIError),
    ],
)
def test_errors_cli(error, classinfo):
    """Ensure that exception types remain consistent."""
    assert issubclass(error, IsilonHadoopToolError)
    assert issubclass(error, cli.CLIError)
    assert issubclass(error, classinfo)
