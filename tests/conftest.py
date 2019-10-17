"""Define config and fixtures for testing the functionality of isilon_hadoop_tools."""


from __future__ import absolute_import
from __future__ import unicode_literals

try:
    import configparser  # Python 3
except ImportError:
    import ConfigParser as configparser  # Python 2
from contextlib2 import ExitStack as does_not_raise
from enum import Enum
import json
import os
import random
import tempfile
try:
    from unittest.mock import Mock, patch  # Python 3
except ImportError:
    from mock import Mock, patch  # Python 2
import uuid

import kadmin
import pytest
import requests
import urllib3

from isilon_hadoop_tools import directories, identities, onefs, IsilonHadoopToolError


urllib3.disable_warnings()  # Without this, the SDK will emit InsecureRequestWarning on every call.


def pytest_addoption(parser):
    parser.addoption(
        '--address',
        help='OneFS Address',
    )
    parser.addoption(
        '--password',
        help='OneFS Admin Password',
    )
    parser.addoption(
        '--username',
        default='root',
        help='OneFS Admin Username',
    )
    parser.addoption(
        '--realm',
        help='Kerberos Realm',
    )
    parser.addoption(
        '--kadmin-address',
        help='Kerberos Administration Server Address',
    )
    parser.addoption(
        '--kdc-addresses',
        help='Kerberos Key Distribution Center Addresses',
        nargs='+',
    )
    parser.addoption(
        '--kadmin-username',
        help='Kerberos Administration Server Admin Username',
    )
    parser.addoption(
        '--kadmin-password',
        help='Kerberos Administration Server Admin Password',
    )


@pytest.fixture
def max_retry_exception_mock():
    """Get an object that raises MaxRetryError (from urllib3) when called."""
    return Mock(side_effect=urllib3.exceptions.MaxRetryError(pool=None, url=None))


@pytest.fixture(
    params=[
        'unresolvable.invalid',  # unresolvable
        'localhost',  # resolvable, not OneFS
        '127.0.0.1',  # IPv4, not OneFS
        '::1',  # IPv6, not OneFS -- If IPv6 is not enabled, this is the same as "unresolvable".
    ],
)
def invalid_address(request, max_retry_exception_mock):
    """Get an address that will cause connection errors for onefs.Client."""
    try:
        # This is how the SDK checks whether localhost is OneFS:
        # https://github.com/Isilon/isilon_sdk_python/blob/19958108ec550865ebeb1f2a4d250322cf4681c2/isi_sdk/rest.py#L33
        __import__('isi.rest')
    except ImportError:
        # Different hostnames/addresses hit errors in different code paths.
        # The first error that can be hit is a socket.gaierror if a hostname is unresolvable.
        # That won't get hit for addresses (e.g. 127.0.0.1 or ::1) or resolvable names, though.
        # Instead, those connections will succeed but will not respond correctly to API requests.
        # The first API request that's made is to get the cluster version (using isi_sdk_8_0).
        # To avoid having to wait for such a connection to time out, here we patch that request.
        with patch('isi_sdk_8_0.ClusterApi.get_cluster_version', max_retry_exception_mock):
            yield request.param  # yield to keep the patch until the teardown of the test.
    else:
        pytest.skip('Localhost is OneFS.')


@pytest.fixture(scope='session')
def onefs_client(pytestconfig):
    """Get an instance of onefs.Client."""
    return onefs.Client(
        address=pytestconfig.getoption('--address', skip=True),
        username=pytestconfig.getoption('--username', skip=True),
        password=pytestconfig.getoption('--password', skip=True),
        verify_ssl=False,  # OneFS uses a self-signed certificate by default.
    )


@pytest.fixture(scope='session')
def riptide_client(onefs_client):
    """Get an instance of onefs.Client that points to Riptide."""
    if onefs.ONEFS_RELEASES['8.0.0.0'] <= onefs_client.revision() < onefs.ONEFS_RELEASES['8.0.1.0']:
        return onefs_client
    pytest.skip('The OneFS cluster is not running Riptide.')


def new_name(request):
    """Get a name that may be used to create a new user or group."""
    return '-'.join([
        request.function.__name__,
        str(uuid.uuid4()),
    ])


def _new_group_name(request):
    return new_name(request)


@pytest.fixture
def new_group_name(request):
    """Get a name that may be used to create a new group."""
    return _new_group_name(request)


def new_id():
    """Get an ID that may be used to create a new user or group."""
    return random.randint(1024, 65536)


def _new_gid():
    return new_id()


@pytest.fixture
def new_gid():
    """Get a GID that may be used to create a new group."""
    return _new_gid()


def _deletable_group(request, onefs_client):
    name, gid = _new_group_name(request), _new_gid()
    onefs_client.create_group(name=name, gid=gid)
    return name, gid


@pytest.fixture
def deletable_group(request, onefs_client):
    """Get the name of an existing group that it is ok to delete."""
    return _deletable_group(request, onefs_client)


def _created_group(request, onefs_client):
    name, gid = _deletable_group(request, onefs_client)
    request.addfinalizer(lambda: onefs_client.delete_group(name='GID:' + str(gid)))
    return name, gid


@pytest.fixture
def created_group(request, onefs_client):
    """Get an existing group with a known GID."""
    return _created_group(request, onefs_client)


def _new_user_name(request):
    return new_name(request)


@pytest.fixture
def new_user_name(request):
    """Get a name that may be used to create a new user."""
    return _new_user_name(request)


def _new_uid():
    return new_id()


@pytest.fixture
def new_uid():
    """Get a UID that may be used to create a new user."""
    return _new_uid()


def _deletable_user(request, onefs_client):
    name = _new_user_name(request)
    primary_group_name, _ = _created_group(request, onefs_client)
    uid = _new_uid()
    onefs_client.create_user(name=name, primary_group_name=primary_group_name, uid=uid)
    return name, primary_group_name, uid


@pytest.fixture
def deletable_user(request, onefs_client):
    """Get the name of an existing user that it is ok to delete."""
    return _deletable_user(request, onefs_client)


def _created_user(request, onefs_client):
    name, primary_group_name, uid = _deletable_user(request, onefs_client)
    request.addfinalizer(lambda: onefs_client.delete_user(name='UID:' + str(uid)))
    return name, primary_group_name, uid


@pytest.fixture
def created_user(request, onefs_client):
    """Get an existing user with a known UID."""
    return _created_user(request, onefs_client)


def _deletable_proxy_user(request, onefs_client):
    user_name = _created_user(request, onefs_client)[0]
    members = []
    onefs_client.create_hdfs_proxy_user(name=user_name, members=members)
    return user_name, members


@pytest.fixture
def deletable_proxy_user(request, onefs_client):
    """Get the name of an existing proxy user that it is ok to delete."""
    return _deletable_proxy_user(request, onefs_client)


def _created_proxy_user(request, onefs_client):
    user_name, members = _deletable_proxy_user(request, onefs_client)
    request.addfinalizer(lambda: onefs_client.delete_hdfs_proxy_user(name=user_name))
    return user_name, members


@pytest.fixture
def created_proxy_user(request, onefs_client):
    """Get an existing proxy user with known members."""
    return _created_proxy_user(request, onefs_client)


def _deletable_realm(pytestconfig, onefs_client):
    realm = pytestconfig.getoption('--realm', skip=True)
    onefs_client.create_realm(
        name=realm,
        admin_server=pytestconfig.getoption('--kadmin-address', skip=True),
        kdcs=pytestconfig.getoption('--kdc-addresses', skip=True),
    )
    return realm


@pytest.fixture
def deletable_realm(pytestconfig, onefs_client):
    """Get the name of an existing realm that it is ok to delete."""
    return _deletable_realm(pytestconfig, onefs_client)


def _created_realm(request, onefs_client):
    realm = _deletable_realm(request.config, onefs_client)
    request.addfinalizer(lambda: onefs_client.delete_realm(name=realm))
    return realm


@pytest.fixture
def created_realm(request, onefs_client):
    """Get the name of an existing realm."""
    return _created_realm(request, onefs_client)


def _deletable_auth_provider(request, onefs_client):
    realm = _created_realm(request, onefs_client)
    onefs_client.create_auth_provider(
        realm=realm,
        user=request.config.getoption('--kadmin-username', skip=True),
        password=request.config.getoption('--kadmin-password', skip=True),
    )
    return realm


@pytest.fixture
def deletable_auth_provider(request, onefs_client):
    """Get the name of an existing Kerberos auth provider that it is ok to delete."""
    return _deletable_auth_provider(request, onefs_client)


def _created_auth_provider(request, onefs_client):
    auth_provider = _deletable_auth_provider(request, onefs_client)
    request.addfinalizer(lambda: onefs_client.delete_auth_provider(name=auth_provider))
    return auth_provider


@pytest.fixture
def created_auth_provider(request, onefs_client):
    """Get the name of an existing Kerberos auth provider."""
    return _created_auth_provider(request, onefs_client)


def _new_spn(request, onefs_client):
    return (
        _new_user_name(request) + '/' + onefs_client.address,
        _created_auth_provider(request, onefs_client),
    )


@pytest.fixture
def new_spn(request, onefs_client):
    """Get a principal that may be used to create a new SPN."""
    return _new_spn(request, onefs_client)


def _remove_principal_from_kdc(
        principal,
        realm,
        kdc,
        admin_server,
        admin_principal,
        admin_password,
):
    """Delete a Kerberos principal."""
    # Note: kadmin.init_with_password requires a Kerberos config file.

    # Create a temporary Kerberos config file.
    krb5_config = configparser.ConfigParser()
    krb5_config.optionxform = str
    krb5_config.add_section('libdefaults')
    krb5_config.set('libdefaults', 'default_realm', realm)
    krb5_config.add_section('realms')
    krb5_config.set(
        'realms',
        realm,
        '\n'.join([
            '{',
            '    kdc = ' + kdc,
            '    admin_server = ' + admin_server,
            '}',
        ]),
    )
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as krb5_conf:
        krb5_config.write(krb5_conf)

    # Activate the config file via an env var.
    previous_krb5_conf = os.environ.get('KRB5_CONFIG')
    os.environ['KRB5_CONFIG'] = krb5_conf.name

    # Delete the principal.
    kadmin.init_with_password(admin_principal, admin_password).delete_principal(principal)

    # Reset the env var.
    if previous_krb5_conf is None:
        del os.environ['KRB5_CONFIG']
    else:
        os.environ['KRB5_CONFIG'] = previous_krb5_conf

    # Delete the config file.
    os.remove(krb5_conf.name)


def _deletable_spn(request, onefs_client):
    spn, auth_provider = _new_spn(request, onefs_client)
    kadmin_username = request.config.getoption('--kadmin-username', skip=True)
    kadmin_password = request.config.getoption('--kadmin-password', skip=True)
    onefs_client.create_spn(
        spn=spn,
        realm=auth_provider,
        user=kadmin_username,
        password=kadmin_password,
    )
    request.addfinalizer(
        lambda: _remove_principal_from_kdc(
            principal=spn,
            realm=auth_provider,
            kdc=request.config.getoption('--kdc-addresses', skip=True)[0],
            admin_server=request.config.getoption('--kadmin-address', skip=True),
            admin_principal=kadmin_username,
            admin_password=kadmin_password,
        ),
    )
    return spn, auth_provider


@pytest.fixture
def deletable_spn(request, onefs_client):
    """Get the name of an existing SPN that it is ok to delete."""
    spn, auth_provider = _deletable_spn(request, onefs_client)
    yield spn, auth_provider
    assert (spn + '@' + auth_provider) not in onefs_client.list_spns(provider=auth_provider)


@pytest.fixture
def created_spn(request, onefs_client):
    """Get the name of an existing Kerberos SPN."""
    spn, auth_provider = _deletable_spn(request, onefs_client)
    request.addfinalizer(lambda: onefs_client.delete_spn(spn=spn, provider=auth_provider))
    return spn, auth_provider


@pytest.fixture
def exception():
    """Get an exception."""
    return random.choice([
        Exception,
        IsilonHadoopToolError,
    ])


def _api_exception_from_http_resp(onefs_client, body):
    return onefs_client._sdk.rest.ApiException(
        http_resp=urllib3.response.HTTPResponse(body=body),
    )


def _api_exception(onefs_client, messages=()):
    return _api_exception_from_http_resp(
        onefs_client,
        body=json.dumps({
            'errors': [
                {'message': message}
                for message in messages
            ],
        }),
    )


@pytest.fixture
def empty_api_exception_mock(onefs_client):
    """Get an object that raises an ApiException (from the Isilon SDK) when called."""
    return Mock(side_effect=_api_exception(onefs_client, messages=['']))


@pytest.fixture
def retriable_api_exception_mock(onefs_client):
    """Get an object that raises a retriable ApiException (from the Isilon SDK) when called."""
    return_value = None
    return (
        Mock(
            side_effect=[
                # First raise an exception, then return a value.
                _api_exception(onefs_client, messages=[onefs.APIError.try_again_error_format]),
                return_value,
            ],
        ),
        return_value,
    )


@pytest.fixture(
    params=[
        lambda onefs_client: (
            # body, decodable, valid, iterable, not empty, valid
            # This is known to occur in the wild.
            _api_exception(onefs_client, messages=[onefs.APIError.try_again_error_format]),
            does_not_raise(),
        ),
        lambda onefs_client: (
            # body, decodable, valid, iterable, not empty, invalid (KeyError)
            _api_exception_from_http_resp(onefs_client, body='{"errors": [{}]}'),
            pytest.raises(onefs.MalformedAPIError),
        ),
        lambda onefs_client: (
            # body, decodable, valid, iterable, not empty, invalid (TypeError)
            _api_exception_from_http_resp(onefs_client, body='{"errors": [[]]}'),
            pytest.raises(onefs.MalformedAPIError),
        ),
        lambda onefs_client: (
            # body, decodable, valid, iterable, empty
            _api_exception_from_http_resp(onefs_client, body='{"errors": []}'),
            does_not_raise(),
        ),
        lambda onefs_client: (
            # body, decodable, valid, not iterable
            _api_exception_from_http_resp(onefs_client, body='{"errors": null}'),
            pytest.raises(onefs.MalformedAPIError),
        ),
        lambda onefs_client: (
            # body, decodable, invalid (KeyError)
            # This is known to occur in the wild (e.g. bug 248011)
            _api_exception_from_http_resp(onefs_client, body='{}'),
            pytest.raises(onefs.MalformedAPIError),
        ),
        lambda onefs_client: (
            # body, decodable, invalid (TypeError)
            _api_exception_from_http_resp(onefs_client, body='[]'),
            pytest.raises(onefs.MalformedAPIError),
        ),
        lambda onefs_client: (
            # body, undecodable
            # This is known to occur in the wild (e.g. if Apache errors before PAPI).
            _api_exception_from_http_resp(onefs_client, body='not JSON'),
            pytest.raises(onefs.UndecodableAPIError),
        ),
        lambda onefs_client: (
            # no body
            # This is known to occur in the wild.
            onefs_client._sdk.rest.ApiException(status=0, reason='built without http_resp'),
            pytest.raises(onefs.UndecodableAPIError),
        ),
        lambda onefs_client: (
            # uninitialized
            onefs_client._sdk.rest.ApiException(),
            pytest.raises(onefs.UndecodableAPIError),
        ),
    ],
)
def api_error_errors_expectation(request, onefs_client):
    """
    Get an APIError and the expectation (context manager)
    of what happens when the errors method is called.
    """
    api_exception, expectation = request.param(onefs_client)
    return onefs.APIError(api_exception), expectation


@pytest.fixture
def api_error(api_error_errors_expectation):
    """Get a onefs.APIError exception."""
    return api_error_errors_expectation[0]


MAX_MODE = 0o1777


@pytest.fixture
def max_mode():
    """Get the highest integer mode this test suite will use."""
    return MAX_MODE


def _deletable_directory(request, onefs_client):
    path = '/' + new_name(request)
    mode = random.randint(0, MAX_MODE)
    mode &= 0o777  # https://bugs.west.isilon.com/show_bug.cgi?id=250615
    onefs_client.mkdir(path=path, mode=mode)
    return path, {
        'group': onefs_client.primary_group_of_user(onefs_client.username),
        'mode': mode,
        'owner': onefs_client.username,
    }


@pytest.fixture
def deletable_directory(request, onefs_client):
    """Get the path and mode of an existing directory that it is ok to delete."""
    return _deletable_directory(request, onefs_client)


def _created_directory(request, onefs_client):
    path, permissions = _deletable_directory(request, onefs_client)
    request.addfinalizer(lambda: onefs_client.rmdir(path=path, recursive=True))
    return path, permissions


@pytest.fixture
def created_directory(request, onefs_client):
    """Get an existing directory with a known mode."""
    return _created_directory(request, onefs_client)


@pytest.fixture
def supported_feature():
    """Get a OneFSFeature that is guaranteed to be supported."""
    return onefs.OneFSFeature.FOREVER


@pytest.fixture
def unsupported_feature():
    """Get a OneFSFeature that is guaranteed to be unsupported."""
    class OneFSFakeFeature(Enum):
        FAKE_FEATURE = (float('inf'), 0)
    return OneFSFakeFeature.FAKE_FEATURE


@pytest.fixture
def requests_delete_raises():
    class _DummyResponse(object):
        def raise_for_status(self):
            raise requests.exceptions.HTTPError
    with patch('requests.delete', lambda *args, **kwargs: _DummyResponse()):
        yield


@pytest.fixture(params=['cdh', 'hdp'])
def users_groups_for_directories(request, onefs_client):
    """
    Get users and groups from the identities module that
    correspond to directories from the directories module
    (i.e. get the identities guaranteed to exist for a set of directories).
    """

    users, groups = set(), set()

    def _pass(*args, **kwargs):
        pass

    identities.iterate_identities(
        {
            'cdh': identities.cdh_identities,
            'hdp': identities.hdp_identities,
        }[request.param](onefs_client.zone),
        create_group=lambda group_name: groups.add(group_name),
        create_user=lambda user_name, _: users.add(user_name),
        add_user_to_group=_pass,
        create_proxy_user=_pass,
    )
    return (
        (users, groups),
        {
            'cdh': directories.cdh_directories,
            'hdp': directories.hdp_directories,
        }[request.param]()
    )
