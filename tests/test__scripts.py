import posixpath
import subprocess
import uuid

import pytest


@pytest.fixture
def empty_hdfs_root(onefs_client):
    """Create a temporary directory and make it the HDFS root."""
    old_hdfs_root = onefs_client.hdfs_settings()['root_directory']
    new_root_name = str(uuid.uuid4())
    onefs_client.mkdir(new_root_name, 0o755)
    onefs_client.update_hdfs_settings({
        'root_directory': posixpath.join(onefs_client.zone_settings()['path'], new_root_name),
    })
    yield
    onefs_client.update_hdfs_settings({'root_directory': old_hdfs_root})
    onefs_client.rmdir(new_root_name, recursive=True)


@pytest.mark.usefixtures('empty_hdfs_root')
@pytest.mark.parametrize('script', ['isilon_create_users', 'isilon_create_directories'])
@pytest.mark.parametrize('dist', ['cdh', 'hdp'])
def test_dry_run(script, onefs_client, dist):
    subprocess.check_call([
        script,
        '--append-cluster-name', str(uuid.uuid4()),
        '--dist', dist,
        '--dry',
        '--no-verify',
        '--onefs-password', onefs_client.password,
        '--onefs-user', onefs_client.username,
        '--zone', 'System',
        onefs_client.address,
    ])
