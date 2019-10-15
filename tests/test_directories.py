import pytest

from isilon_hadoop_tools import IsilonHadoopToolError, directories


def test_directory_identities(users_groups_for_directories):
    """
    Verify that identities needed by the directories module
    are guaranteed to exist by the identities module.
    """
    (users, groups), dirs = users_groups_for_directories
    for hdfs_directory in dirs:
        assert hdfs_directory.owner in users
        assert hdfs_directory.group in groups


@pytest.mark.parametrize(
    'error, classinfo',
    [
        (directories.DirectoriesError, IsilonHadoopToolError),
        (directories.HDFSRootDirectoryError, directories.DirectoriesError),
    ],
)
def test_errors_cli(error, classinfo):
    """Ensure that exception types remain consistent."""
    assert issubclass(error, IsilonHadoopToolError)
    assert issubclass(error, directories.DirectoriesError)
    assert issubclass(error, classinfo)
