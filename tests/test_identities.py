import pytest

import isilon_hadoop_tools.identities


@pytest.mark.parametrize('zone', ['System', 'notSystem'])
@pytest.mark.parametrize(
    'identities',
    [
        isilon_hadoop_tools.identities.cdh_identities,
        isilon_hadoop_tools.identities.hdp_identities,
    ],
)
def test_log_identities(identities, zone):
    """Verify that log_identities returns None."""
    assert isilon_hadoop_tools.identities.log_identities(identities(zone)) is None
