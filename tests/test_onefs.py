"""Verify the functionality of isilon_hadoop_tools.onefs."""


from __future__ import absolute_import
from __future__ import unicode_literals

import socket
try:
    # Python 3
    from unittest.mock import Mock
    from urllib.parse import urlparse
except ImportError:
    # Python 2
    from mock import Mock
    from urlparse import urlparse
import uuid

import isi_sdk_7_2
import isi_sdk_8_0
import isi_sdk_8_0_1
import isi_sdk_8_1_0
import isi_sdk_8_1_1
import isi_sdk_8_2_0
import isi_sdk_8_2_1
import isi_sdk_8_2_2
import pytest

from isilon_hadoop_tools import IsilonHadoopToolError, onefs


def test_init_connection_error(invalid_address, pytestconfig):
    """Creating a Client for an unusable host should raise a OneFSConnectionError."""
    with pytest.raises(onefs.OneFSConnectionError):
        onefs.Client(address=invalid_address, username=None, password=None)


@pytest.mark.xfail(
    raises=onefs.MalformedAPIError,
    reason='https://bugs.west.isilon.com/show_bug.cgi?id=248011',
)
def test_init_bad_creds(pytestconfig):
    """Creating a Client with invalid credentials should raise an appropriate exception."""
    with pytest.raises(onefs.APIError):
        onefs.Client(
            address=pytestconfig.getoption('--address', skip=True),
            username=str(uuid.uuid4()),
            password=str(uuid.uuid4()),
            verify_ssl=False,  # OneFS uses a self-signed certificate by default.
        )


def test_init(request):
    """Creating a Client should not raise an Exception."""
    assert isinstance(request.getfixturevalue('onefs_client'), onefs.Client)


def test_api_error_errors(api_error_errors_expectation):
    """Verify that APIError.errors raises appropriate exceptions."""
    api_error, expectation = api_error_errors_expectation
    with expectation:
        api_error.errors()


def test_api_error_str(api_error):
    """Verify that APIErrors can be stringified."""
    assert isinstance(str(api_error), str)


@pytest.mark.parametrize(
    'revision, expected_sdk',
    [
        (0, isi_sdk_8_2_2),
        (onefs.ONEFS_RELEASES['7.2.0.0'], isi_sdk_7_2),
        (onefs.ONEFS_RELEASES['8.0.0.0'], isi_sdk_8_0),
        (onefs.ONEFS_RELEASES['8.0.0.4'], isi_sdk_8_0),
        (onefs.ONEFS_RELEASES['8.0.1.0'], isi_sdk_8_0_1),
        (onefs.ONEFS_RELEASES['8.0.1.1'], isi_sdk_8_0_1),
        (onefs.ONEFS_RELEASES['8.1.0.0'], isi_sdk_8_1_0),
        (onefs.ONEFS_RELEASES['8.1.1.0'], isi_sdk_8_1_1),
        (onefs.ONEFS_RELEASES['8.1.2.0'], isi_sdk_8_1_1),
        (onefs.ONEFS_RELEASES['8.2.0.0'], isi_sdk_8_2_0),
        (onefs.ONEFS_RELEASES['8.2.1.0'], isi_sdk_8_2_1),
        (onefs.ONEFS_RELEASES['8.2.2.0'], isi_sdk_8_2_2),
        (onefs.ONEFS_RELEASES['8.2.3.0'], isi_sdk_8_2_2),
        (float('inf'), isi_sdk_8_2_2),
    ],
)
def test_sdk_for_revision(revision, expected_sdk):
    """Verify that an appropriate SDK is selected for a given revision."""
    assert onefs.sdk_for_revision(revision) is expected_sdk


def test_sdk_for_revision_unsupported():
    """Ensure that an UnsupportedVersion exception for unsupported revisions."""
    with pytest.raises(onefs.UnsupportedVersion):
        onefs.sdk_for_revision(revision=0, strict=True)


def test_accesses_onefs_connection_error(max_retry_exception_mock, onefs_client):
    """Verify that MaxRetryErrors are converted to OneFSConnectionErrors."""
    with pytest.raises(onefs.OneFSConnectionError):
        onefs.accesses_onefs(max_retry_exception_mock)(onefs_client)


def test_accesses_onefs_api_error(empty_api_exception_mock, onefs_client):
    """Verify that APIExceptions are converted to APIErrors."""
    with pytest.raises(onefs.APIError):
        onefs.accesses_onefs(empty_api_exception_mock)(onefs_client)


def test_accesses_onefs_try_again(retriable_api_exception_mock, onefs_client):
    """Verify that APIExceptions are retried appropriately."""
    mock, return_value = retriable_api_exception_mock
    assert onefs.accesses_onefs(mock)(onefs_client) == return_value


def test_accesses_onefs_other(exception, onefs_client):
    """Verify that arbitrary exceptions are not caught."""
    with pytest.raises(exception):
        onefs.accesses_onefs(Mock(side_effect=exception))(onefs_client)


def test_address(onefs_client, pytestconfig):
    """Verify that onefs.Client.address is exactly what was passed in."""
    assert onefs_client.address == pytestconfig.getoption('--address')


def test_username(onefs_client, pytestconfig):
    """Verify that onefs.Client.username is exactly what was passed in."""
    assert onefs_client.username == pytestconfig.getoption('--username')


def test_password(onefs_client, pytestconfig):
    """Verify that onefs.Client.password is exactly what was passed in."""
    assert onefs_client.password == pytestconfig.getoption('--password')


def test_host(onefs_client):
    """Verify that onefs.Client.host is a parsable url."""
    parsed = urlparse(onefs_client.host)
    assert parsed.scheme == 'https'
    assert socket.gethostbyname(parsed.hostname)
    assert parsed.port == 8080


def test_create_group(request):
    """Ensure that a group can be created successfully."""
    request.getfixturevalue('created_group')


def test_delete_group(onefs_client, deletable_group):
    """Verify that a group can be deleted successfully."""
    group_name, _ = deletable_group
    assert onefs_client.delete_group(name=group_name) is None


def test_gid_of_group(onefs_client, created_group):
    """Verify that the correct GID is fetched for an existing group."""
    group_name, gid = created_group
    assert onefs_client.gid_of_group(group_name=group_name) == gid


def test_groups(onefs_client, created_group):
    """Verify that a group that is known to exist appears in the list of existing groups."""
    group_name, _ = created_group
    assert group_name in onefs_client.groups()


def test_delete_user(onefs_client, deletable_user):
    """Verify that a user can be deleted successfully."""
    assert onefs_client.delete_user(name=deletable_user[0]) is None


def test_create_user(request):
    """Ensure that a user can be created successfully."""
    request.getfixturevalue('created_user')


def test_add_user_to_group(onefs_client, created_user, created_group):
    """Ensure that a user can be added to a group successfully."""
    assert onefs_client.add_user_to_group(
        user_name=created_user[0],
        group_name=created_group[0],
    ) is None


def test_create_hdfs_proxy_user(request):
    """Ensure that an HDFS proxy user can be created successfully."""
    request.getfixturevalue('created_proxy_user')


def test_delete_proxy_user(onefs_client, deletable_proxy_user):
    """Verify that a proxy user can be deleted successfully."""
    assert onefs_client.delete_hdfs_proxy_user(name=deletable_proxy_user[0]) is None


def test_uid_of_user(onefs_client, created_user):
    """Verify that the correct UID is fetched for an existing user."""
    user_name, _, uid = created_user
    assert onefs_client.uid_of_user(user_name=user_name) == uid


def test_primary_group_of_user(onefs_client, created_user):
    """Verify that the correct primary group is fetched for an existing user."""
    user_name, primary_group, _ = created_user
    assert onefs_client.primary_group_of_user(user_name=user_name) == primary_group


def test_create_realm(request):
    """Verify that a Kerberos realm can be created successfully."""
    request.getfixturevalue('created_realm')


def test_delete_realm(onefs_client, deletable_realm):
    """Verify that a realm can be deleted successfully."""
    onefs_client.delete_realm(name=deletable_realm)


def test_create_auth_provider(request):
    """Verify that a Kerberos auth provider can be created successfully."""
    request.getfixturevalue('created_auth_provider')


def test_delete_auth_provider(onefs_client, deletable_auth_provider):
    """Verify that a Kerberos auth provider can be deleted successfully."""
    onefs_client.delete_auth_provider(name=deletable_auth_provider)


def test_delete_spn(onefs_client, deletable_spn):
    """Verify that an SPN can be deleted successfully."""
    spn, provider = deletable_spn
    onefs_client.delete_spn(spn=spn, provider=provider)


def test_create_spn(request):
    """Verify that a Kerberos SPN can be created successfully."""
    request.getfixturevalue('created_spn')


def test_list_spns(onefs_client, created_spn):
    """Verify that a Kerberos SPN can be listed successfully."""
    spn, provider = created_spn
    assert (spn + '@' + provider) in onefs_client.list_spns(provider=provider)


def test_flush_auth_cache(onefs_client):
    """Verify that flushing the auth cache does not raise an exception."""
    assert onefs_client.flush_auth_cache() is None


def test_flush_auth_cache_unsupported(riptide_client):
    """
    Verify that trying flush the auth cache of a non-System zone
    before Halfpipe raises an UnsupportedOperation exception.
    """
    with pytest.raises(onefs.UnsupportedOperation):
        riptide_client.flush_auth_cache(zone='notSystem')


@pytest.mark.usefixtures('requests_delete_raises')
def test_flush_auth_cache_error(riptide_client):
    """
    Verify that flushing the auth cache raises an appropriate exception
    when things go wrong before Halfpipe.
    """
    with pytest.raises(onefs.NonSDKAPIError):
        riptide_client.flush_auth_cache()


def test_hdfs_inotify_settings(onefs_client):
    """Ensure hdfs_inotify_settings returns all available settings appropriately."""
    try:
        hdfs_inotify_settings = onefs_client.hdfs_inotify_settings()
    except onefs.UnsupportedOperation:
        assert onefs_client.revision() < onefs.ONEFS_RELEASES['8.1.1.0']
    else:
        assert isinstance(hdfs_inotify_settings, dict)
        assert all(
            setting in hdfs_inotify_settings
            for setting in ['enabled', 'maximum_delay', 'retention']
        )


@pytest.mark.parametrize(
    'setting_and_type',
    {
        'alternate_system_provider': str,
        'auth_providers': list,
        'cache_entry_expiry': int,
        'create_path': (bool, type(None)),
        'groupnet': str,
        'home_directory_umask': int,
        'id': str,
        'map_untrusted': str,
        'name': str,
        'netbios_name': str,
        'path': str,
        'skeleton_directory': str,
        'system': bool,
        'system_provider': str,
        'user_mapping_rules': list,
        'zone_id': int,
    }.items(),
)
def test_zone_settings(onefs_client, setting_and_type):
    """Ensure zone_settings returns all available settings appropriately."""
    setting, setting_type = setting_and_type
    assert isinstance(onefs_client.zone_settings()[setting], setting_type)


def test_zone_settings_bad_zone(onefs_client):
    """Ensure zone_settings fails appropriately when given a nonexistent zone."""
    with pytest.raises(onefs.MissingZoneError):
        onefs_client.zone_settings(zone=str(uuid.uuid4()))


def test_mkdir(request, onefs_client):
    """Ensure that a directory can be created successfully."""
    path, permissions = request.getfixturevalue('created_directory')

    def _check_postconditions():
        assert onefs_client.permissions(path) == permissions
    request.addfinalizer(_check_postconditions)


@pytest.mark.parametrize('recursive', [False, True])
def test_rmdir(onefs_client, deletable_directory, recursive, request):
    """Verify that a directory can be deleted successfully."""
    path, _ = deletable_directory
    assert onefs_client.rmdir(path=path, recursive=recursive) is None

    def _check_postconditions():
        with pytest.raises(onefs.APIError):
            onefs_client.permissions(path)
    request.addfinalizer(_check_postconditions)


def test_permissions(onefs_client, created_directory):
    """Check that permissions returns correct information."""
    path, permissions = created_directory
    assert onefs_client.permissions(path) == permissions


def test_chmod(onefs_client, created_directory, max_mode, request):
    """Check that chmod modifies the mode correctly."""
    path, permissions = created_directory
    new_mode = (permissions['mode'] + 1) % (max_mode + 1)
    assert onefs_client.chmod(path, new_mode) is None

    def _check_postconditions():
        assert onefs_client.permissions(path)['mode'] == new_mode
    request.addfinalizer(_check_postconditions)


@pytest.mark.parametrize('new_owner', [True, False])
@pytest.mark.parametrize('new_group', [True, False])
def test_chown(
        onefs_client,
        created_directory,
        created_user,
        created_group,
        new_owner,
        new_group,
        request,
):
    """Check that chown modifies ownership correctly."""
    path, permissions = created_directory
    user_name = created_user[0]
    group_name = created_group[0]
    assert onefs_client.chown(
        path,
        owner=user_name if new_owner else None,
        group=group_name if new_group else None,
    ) is None

    def _check_postconditions():
        owner = user_name if new_owner else permissions['owner']
        assert onefs_client.permissions(path)['owner'] == owner
        group = group_name if new_group else permissions['group']
        assert onefs_client.permissions(path)['group'] == group
    request.addfinalizer(_check_postconditions)


def test_feature_supported(onefs_client, supported_feature):
    """Ensure that feature_is_supported correctly identifies a supported feature."""
    try:
        assert onefs_client.feature_is_supported(supported_feature)
    except onefs.UnsupportedOperation:
        assert onefs_client.revision() < onefs.ONEFS_RELEASES['8.2.0.0']


def test_feature_unsupported(onefs_client, unsupported_feature):
    """Ensure that feature_is_supported correctly identifies an unsupported feature."""
    try:
        assert not onefs_client.feature_is_supported(unsupported_feature)
    except onefs.UnsupportedOperation:
        assert onefs_client.revision() < onefs.ONEFS_RELEASES['8.2.0.0']


@pytest.mark.parametrize(
    'error, classinfo',
    [
        (onefs.APIError, onefs.OneFSError),
        (onefs.ExpiredLicenseError, onefs.MissingLicenseError),
        (onefs.MalformedAPIError, onefs.OneFSError),
        (onefs.MissingLicenseError, onefs.OneFSError),
        (onefs.MissingZoneError, onefs.OneFSError),
        (onefs.MixedModeError, onefs.OneFSError),
        (onefs.OneFSCertificateError, onefs.OneFSConnectionError),
        (onefs.OneFSConnectionError, onefs.OneFSError),
        (onefs.OneFSError, IsilonHadoopToolError),
        (onefs.OneFSValueError, ValueError),
        (onefs.NonSDKAPIError, onefs.OneFSError),
        (onefs.UndecodableAPIError, onefs.MalformedAPIError),
        (onefs.UndeterminableVersion, onefs.OneFSError),
        (onefs.UnsupportedOperation, onefs.OneFSError),
        (onefs.UnsupportedVersion, onefs.OneFSError),
    ],
)
def test_errors_onefs(error, classinfo):
    """Ensure that exception types remain consistent."""
    assert issubclass(error, IsilonHadoopToolError)
    assert issubclass(error, onefs.OneFSError)
    assert issubclass(error, classinfo)
