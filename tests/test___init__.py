"""Verify the functionality of isilon_hadoop_tools.__init__."""


from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

import isilon_hadoop_tools


@pytest.mark.parametrize(
    'error, classinfo',
    [
        (isilon_hadoop_tools.IsilonHadoopToolError, Exception),
    ],
)
def test_errors(error, classinfo):
    """Ensure that exception types remain consistent."""
    assert issubclass(error, isilon_hadoop_tools.IsilonHadoopToolError)
    assert issubclass(error, classinfo)
