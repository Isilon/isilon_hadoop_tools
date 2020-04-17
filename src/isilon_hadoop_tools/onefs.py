"""Classes for Interacting with OneFS"""

# pylint: disable=too-many-lines

from __future__ import absolute_import
from __future__ import unicode_literals

from datetime import date, datetime
from enum import Enum
import json
import logging
import posixpath
import socket
import struct
import time
try:
    from urllib.parse import urlparse, urlunparse  # Python 3
except ImportError:
    from urlparse import urlparse, urlunparse  # Python 2
import urllib3

from future.utils import raise_from
import requests

from isilon_hadoop_tools import IsilonHadoopToolError


__all__ = [
    # Constants / Enums
    'ONEFS_RELEASES',
    'OneFSFeature',

    # Decorators
    'accesses_onefs',

    # Exceptions
    'APIError',
    'ExpiredLicenseError',
    'MalformedAPIError',
    'MissingLicenseError',
    'MissingZoneError',
    'MixedModeError',
    'NonSDKAPIError',
    'OneFSError',
    'OneFSConnectionError',
    'OneFSCertificateError',
    'OneFSValueError',
    'UndecodableAPIError',
    'UndeterminableVersion',
    'UnsupportedOperation',
    'UnsupportedVersion',

    # Functions
    'sdk_for_revision',

    # Objects
    'Client',
]

LOGGER = logging.getLogger(__name__)
ONEFS_RELEASES = {
    '7.2.0.0': 0x70200500000000A,
    '8.0.0.0': 0x800005000000025,
    '8.0.0.4': 0x800005000400035,
    '8.0.1.0': 0x800015000000007,
    '8.0.1.1': 0x800015000100070,
    '8.1.0.0': 0x80100500000000B,
    '8.1.1.0': 0x8010150000000D4,
    '8.1.2.0': 0x801025000000010,
    '8.1.3.0': 0x80103500000000D,
    '8.2.0.0': 0x80200500000000B,
    '8.2.1.0': 0x802015000000004,
    '8.2.2.0': 0x802025000000007,
    '8.2.3.0': 0x802035000000000,
}


class OneFSFeature(Enum):

    """OneFS Feature Flags for Use with Client.feature_is_supported"""

    # These values come from sys/sys/isi_upgrade_api_featuremap.h.
    # For example,
    #  ISI_UPGRADE_API_FEATURE_VERSION(PIPE_UAPI_OVERRIDES, 8, 1, 3, 0)
    # translates to
    #  PIPE_UAPI_OVERRIDES = (0x8010300, 0)

    _GEN = {
        'INIT': 0x0000000,
        'JAWS': 0x7010100,
        'MOBY': 0x7020000,
        'ORCA': 0x7020100,
        'RIP0': 0x7030000,
        'RIP1': 0x7030100,
        'RIPT': 0x8000000,
        'HAPI': 0x8000100,
        'FRTR': 0x8010000,
        'NJMA': 0x8010100,
        'KANA': 0x8010200,
        'NDUU': 0x8010300,
        'PIPE': 0x8020000,
        'ERA1': 0x9000100,
    }

    # pylint: disable=invalid-name
    FOREVER = (_GEN['INIT'], 0)

    JAWS_RU = (_GEN['JAWS'], 0)

    MOBY_PROTECTION = (_GEN['MOBY'], 0)
    MOBY_SNAPDELETE = (_GEN['MOBY'], 1)
    MOBY_RU = (_GEN['MOBY'], 2)
    MOBY_UNFS = (_GEN['MOBY'], 3)
    MOBY_AUTH_UPGRADE = (_GEN['MOBY'], 4)

    ORCA_RU = (_GEN['ORCA'], 0)
    RIPT_CONSISTENT_HASH = (_GEN['ORCA'], 1)

    RIPT_RBM_VERSIONING = (_GEN['RIP0'], 0)

    BATCH_ERROR_DSR = (_GEN['RIP1'], 1)
    RIPTIDE_MEDIASCAN = (_GEN['RIP1'], 2)
    RIPT_8K_INODES = (_GEN['RIP1'], 3)

    RIPT_DEDUPE = (_GEN['RIPT'], 0)
    RIPTIDE_TRUNCATE = (_GEN['RIPT'], 1)
    RIPTIDE_CHANGELISTCREATE = (_GEN['RIPT'], 2)
    RIPT_GMP_SERVICES = (_GEN['RIPT'], 3)
    RIPT_NLM = (_GEN['RIPT'], 4)
    RIPTIDE_FSA = (_GEN['RIPT'], 5)
    RIPT_SMARTPOOLS = (_GEN['RIPT'], 6)
    RIPT_AUTH_UPGRADE = (_GEN['RIPT'], 7)
    RIPT_CELOG_UPGRADE = (_GEN['RIPT'], 8)

    HALFPIPE_PARTITIONED_PERFORMANCE = (_GEN['HAPI'], 0)
    HP_JE = (_GEN['HAPI'], 1)
    HP_WORM = (_GEN['HAPI'], 2)
    HP_PROXY = (_GEN['HAPI'], 3)
    HALFPIPE_CONTAINERS = (_GEN['HAPI'], 4)
    HP_NEEDS_NDU_FLAG = (_GEN['HAPI'], 5)
    HP_RANGER = (_GEN['HAPI'], 6)
    HP_AMBARI_METRICS = (_GEN['HAPI'], 7)
    HP_DATANODE_WIRE_ENCRYPTION = (_GEN['HAPI'], 8)

    FT_SMARTPOOLS = (_GEN['FRTR'], 0)
    FRT_MIRRORED_JOURNAL = (_GEN['FRTR'], 1)
    FRT_LIN_SUPER_DRIVE_QUORUM = (_GEN['FRTR'], 2)
    FREIGHT_TRAINS_LAYOUT = (_GEN['FRTR'], 3)
    FT_ESRS = (_GEN['FRTR'], 4)
    FRT_COMPRESSED_INODES = (_GEN['FRTR'], 5)
    FTR_LICENSE_MIGRATION = (_GEN['FRTR'], 6)

    PIPE_ITER_MARK = (_GEN['NJMA'], 0)
    NIIJIMA_CPOOL_GOOGLE_XML = (_GEN['NJMA'], 1)
    NJMA_HDFS_INOTIFY = (_GEN['NJMA'], 2)
    NJMA_HDFS_FSIMAGE = (_GEN['NJMA'], 3)
    NIIJIMA_CLUSTER_TIME = (_GEN['NJMA'], 4)
    NIIJIMA_SMB = (_GEN['NJMA'], 5)
    NIIJIMA_ESRS = (_GEN['NJMA'], 6)

    KANA_HDFS_REF_BY_INODE = (_GEN['KANA'], 0)
    KANA_WEBHDFS_DELEGATION_TOKENS = (_GEN['KANA'], 1)

    PIPE_UAPI_OVERRIDES = (_GEN['NDUU'], 0)

    PIPE_AUTH_AWS_V4 = (_GEN['PIPE'], 0)
    PIPE_HANGDUMP = (_GEN['PIPE'], 1)
    PIPE_GMP_CFG_GEN = (_GEN['PIPE'], 2)
    PIPE_EXT_GROUP = (_GEN['PIPE'], 3)
    PIPE_EXT_GROUP_MSG = (_GEN['PIPE'], 4)
    PIPE_JE = (_GEN['PIPE'], 5)
    PIPE_IFS_DOMAINS = (_GEN['PIPE'], 6)
    PIPE_CPOOL_SECURE_KEY = (_GEN['PIPE'], 7)
    PIPE_CPOOL_C2S = (_GEN['PIPE'], 8)
    PIPE_ISI_CERTS = (_GEN['PIPE'], 9)
    PIPE_NDMP = (_GEN['PIPE'], 10)
    PIPE_ZONED_ROLES = (_GEN['PIPE'], 11)
    FT_JE_ZOMBIE = (_GEN['PIPE'], 12)
    PIPE_CPOOL_GOOGLE_XML = (_GEN['PIPE'], 13)
    PIPE_SIQ = (_GEN['PIPE'], 14)
    PIPE_QUOTAS_MS = (_GEN['PIPE'], 15)
    PIPE_QUOTA_USER_CONTAINERS = (_GEN['PIPE'], 16)
    PIPE_TREEDELETE = (_GEN['PIPE'], 17)
    PIPE_DOMAIN_SNAPSHOTS = (_GEN['PIPE'], 18)
    PIPE_QUOTA_DDQ = (_GEN['PIPE'], 19)
    PIPE_ARRAYD = (_GEN['PIPE'], 20)
    PIPE_FLEXNET_V4 = (_GEN['PIPE'], 21)
    PIPE_ISI_DAEMON_IPV6 = (_GEN['PIPE'], 22)
    PIPE_JE_PREP = (_GEN['PIPE'], 23)
    PIPE_IFS_LFN = (_GEN['PIPE'], 24)
    PIPE_SNAP_SCHED_TARDIS = (_GEN['PIPE'], 25)
    PIPE_DRIVE_INTEROP = (_GEN['PIPE'], 26)
    PIPE_READ_BLOCKS = (_GEN['PIPE'], 27)
    PIPE_IFS_BCM = (_GEN['PIPE'], 28)
    PIPE_EXT_GRP_SRO = (_GEN['PIPE'], 29)
    PIPE_HDFS_EXTATTR = (_GEN['PIPE'], 30)
    PIPE_CP_2_0 = (_GEN['PIPE'], 31)
    PIPE_FILEPOLICY = (_GEN['PIPE'], 32)
    PIPE_COAL_SUSP_AGGR = (_GEN['PIPE'], 34)
    PIPE_SMARTCONNECT_DNS = (_GEN['PIPE'], 35)
    PIPE_PDM_ENC_INATTR = (_GEN['PIPE'], 36)
    PIPE_NDMP_REDIRECTOR = (_GEN['PIPE'], 38)
    PIPE_ISI_CBIND_D = (_GEN['PIPE'], 39)
    PIPE_SPARSE_PUNCH = (_GEN['PIPE'], 41)
    PIPE_SSH_CONFIG = (_GEN['PIPE'], 42)
    PIPE_AUDIT_EVENTS = (_GEN['PIPE'], 43)
    PIPE_PURPOSEDB = (_GEN['PIPE'], 45)
    PIPE_JE_TREEWALK = (_GEN['PIPE'], 47)

    ERA1_HDFS_TDE = (_GEN['ERA1'], 1)
    ERA1_QUOTA_APPLOGICAL = (_GEN['ERA1'], 4)
    ERA1_IDI_VERIFY_SNAPID = (_GEN['ERA1'], 6)
    ERA1_CPOOL_ALIYUN = (_GEN['ERA1'], 7)
    ERA1_STF_DUMMY_LINS = (_GEN['ERA1'], 8)
    ERA1_PDM_COLLECT = (_GEN['ERA1'], 13)
    ERA1_MCP_MLIST = (_GEN['ERA1'], 14)
    ERA1_NFS_SCHED_CONFIG = (_GEN['ERA1'], 16)
    ERA1_ADS_VOPS = (_GEN['ERA1'], 17)
    ERA1_GMP_SERVICE_LSASS = (_GEN['ERA1'], 18)
    ERA1_SINLIN_LOCK_ORDER = (_GEN['ERA1'], 20)
    ERA1_LIN_MASTER_FLAGS = (_GEN['ERA1'], 23)
    ERA1_REMOTE_SYSCTL_OBJECT = (_GEN['ERA1'], 25)
    ERA1_LIN_BUCKET_LOCK = (_GEN['ERA1'], 27)
    ERA1_PDM_SNAPGOV_RENAME = (_GEN['ERA1'], 34)
    # pylint: enable=invalid-name


class OneFSError(IsilonHadoopToolError):
    """All Exceptions emitted from this module inherit from this Exception."""


class OneFSConnectionError(OneFSError):
    """
    This Exception is raised when a client cannot connect to OneFS
    (e.g. due to socket.gaierror or urllib3.exceptions.MaxRetryError).
    """


class OneFSCertificateError(OneFSConnectionError):
    """This exception occurs when a client cannot connect due to an invalid HTTPS certificate."""


class NonSDKAPIError(OneFSError):
    """This exception is raised when interacting with OneFS APIs without the SDK fails."""


class _BaseAPIError(OneFSError):

    def __init__(self, exc):
        super(_BaseAPIError, self).__init__(str(exc))
        self.exc = exc


class MalformedAPIError(_BaseAPIError):
    """This exception wraps an Isilon SDK ApiException that does not have a valid JSON body."""


class UndecodableAPIError(MalformedAPIError):
    """This exception wraps an Isilon SDK ApiException that does not have a JSON-decodable body."""


class APIError(_BaseAPIError):

    """This exception wraps an Isilon SDK ApiException."""

    # pylint: disable=invalid-name
    gid_already_exists_error_format = "Group already exists with gid '{0}'"
    group_already_exists_error_format = "Group '{0}' already exists"
    group_not_found_error_format = "Failed to find group for 'GROUP:{0}': No such group"
    group_unresolvable_error_format = "Could not resolve group {0}"
    license_expired_error_format = (
        # Note: Subscriptions did not exist prior to Freight Trains,
        # so old code assumes that only evaluations have expiration dates.
        "The evaluation license key for {0} has expired."
        " Please contact your Isilon representative."
    )
    license_missing_error_format = (
        "The {0} application is not currently installed."
        " Please contact your Isilon account team for"
        " more information on evaluating and purchasing {0}."
    )
    proxy_user_already_exists_error_format = "Proxyuser '{0}' already exists"
    try_again_error_format = "OneFS API is temporarily unavailable. Try your request again."
    uid_already_exists_error_format = "User already exists with uid '{0}'"
    user_already_exists_error_format = "User '{0}' already exists"
    user_already_in_group_error_format = \
        "Failed to add member UID:{0} to group GROUP:{1}: User is already in local group"
    user_not_found_error_format = "Failed to find user for 'USER:{0}': No such user"
    user_unresolvable_error_format = "Could not resolve user {0}"
    zone_not_found_error_format = 'Access Zone "{0}" not found.'
    dir_path_already_exists_error_format = \
        'Unable to create directory as requested -- container already exists'
    # pylint: enable=invalid-name

    def __str__(self):
        try:
            return '\n'.join(error['message'] for error in self.errors()) or str(self.exc)
        except MalformedAPIError as exc:
            return str(exc)

    def errors(self):
        """Get errors listed in the exception."""
        try:
            json_body = json.loads(self.exc.body)
        except (
                TypeError,   # self.exc.body is not a str.
                ValueError,  # self.exc.body is not JSON.
        ) as exc:
            raise_from(UndecodableAPIError(self.exc), exc)
        try:
            for error in json_body['errors']:
                # Raise a KeyError if 'message' is not in error:
                error['message']  # pylint: disable=pointless-statement
        except (
                KeyError,   # 'errors' or 'message' is not in json_body or error, respectively.
                TypeError,  # json_body['errors'] is not iterable.
        ) as exc:
            raise_from(MalformedAPIError(self.exc), exc)
        return json_body['errors']

    def filtered_errors(self, filter_func):
        """Arbitrarily filter errors in the exception."""
        for error in self.errors():
            if filter_func(error):
                yield error

    def gid_already_exists_error(self, gid):
        """Returns True if the exception contains a GID already exists error."""
        return any(
            self.filtered_errors(
                lambda error: error['message'] == self.gid_already_exists_error_format.format(gid),
            )
        )

    def group_already_exists_error(self, group_name):
        """Returns True if the exception contains a group already exists error."""
        return any(
            self.filtered_errors(
                lambda error: error['message'] == self.group_already_exists_error_format.format(
                    group_name,
                ),
            )
        )

    def group_not_found_error(self, group_name):
        """Returns True if the exception contains a group not found error."""
        return any(
            self.filtered_errors(
                lambda error: error['message'] == self.group_not_found_error_format.format(
                    group_name,
                ),
            )
        )

    def group_unresolvable_error(self, group_name):
        """Returns True if the exception contains an unresolvable group error."""
        return any(
            self.filtered_errors(
                lambda error: error['message'] == self.group_unresolvable_error_format.format(
                    group_name,
                ),
            )
        )

    def license_expired_error(self, license_name):
        """Returns True if the exception contains an expired license error."""
        return any(
            self.filtered_errors(
                lambda error: error['message'] == self.license_expired_error_format.format(
                    license_name,
                ),
            )
        )

    def license_missing_error(self, license_name):
        """Returns True if the exception contains a missing license error."""
        return any(
            self.filtered_errors(
                lambda error: error['message'] == self.license_missing_error_format.format(
                    license_name,
                ),
            )
        )

    def proxy_user_already_exists_error(self, proxy_user_name):
        """Returns True if the exception contains a proxy user already exists error."""
        return any(
            self.filtered_errors(
                lambda error:
                error['message'] == self.proxy_user_already_exists_error_format.format(
                    proxy_user_name,
                ),
            )
        )

    def try_again_error(self):
        """Returns True if the exception indicated PAPI is temporarily unavailable."""
        return any(
            self.filtered_errors(
                lambda error:
                error['message'] == self.try_again_error_format,
            )
        )

    def uid_already_exists_error(self, uid):
        """Returns True if the exception contains a UID already exists error."""
        return any(
            self.filtered_errors(
                lambda error: error['message'] == self.uid_already_exists_error_format.format(uid),
            )
        )

    def user_already_exists_error(self, user_name):
        """Returns True if the exception contains a user already exists error."""
        return any(
            self.filtered_errors(
                lambda error: error['message'] == self.user_already_exists_error_format.format(
                    user_name,
                ),
            )
        )

    def user_already_in_group_error(self, uid, group_name):
        """Returns True if the exception contains a user already in group error."""
        return any(
            self.filtered_errors(
                lambda error: error['message'] == self.user_already_in_group_error_format.format(
                    uid,
                    group_name,
                ),
            )
        )

    def user_not_found_error(self, user_name):
        """Returns True if the exception contains a user not found error."""
        return any(
            self.filtered_errors(
                lambda error: error['message'] == self.user_not_found_error_format.format(
                    user_name,
                ),
            )
        )

    def user_unresolvable_error(self, user_name):
        """Returns True if the exception contains an unresolvable user error."""
        return any(
            self.filtered_errors(
                lambda error: error['message'] == self.user_unresolvable_error_format.format(
                    user_name,
                ),
            )
        )

    def zone_not_found_error(self, zone_name):
        """Returns True if the exception contains a zone not found error."""
        return any(
            self.filtered_errors(
                lambda error: error['message'] == self.zone_not_found_error_format.format(
                    zone_name,
                ),
            )
        )

    def dir_path_already_exists_error(self):
        """Returns True if the exception contains a directory path already exist error."""
        return any(
            self.filtered_errors(
                lambda error: error['message'] == self.dir_path_already_exists_error_format,
            )
        )


class MissingLicenseError(OneFSError):
    """This Exception is raised when a license that is expected to exist cannot be found."""


class ExpiredLicenseError(MissingLicenseError):
    """This Exception is raised when a license has expired."""


class MissingZoneError(OneFSError):
    """This Exception is raised when a zone that is expected to exist cannot be found."""


class MixedModeError(OneFSError):
    """
    This Exception is raised when an operation cannot succeed due to
    the cluster containing nodes running different versions of OneFS.
    """


class UndeterminableVersion(OneFSError):
    """
    This Exception is raised when attempting to use this
    module with a version of OneFS that cannot be determined (usually < 8.0.0.0).
    """


class UnsupportedVersion(OneFSError):
    """
    This Exception is raised when attempting to use this
    module with an unsupported version of OneFS.
    """


class UnsupportedOperation(OneFSError):
    """
    This Exception is raised when attempting to conduct an unsupported
    operation with an specific version of OneFS.
    """


def sdk_for_revision(revision, strict=False):
    """Get the SDK that is intended to work with a given OneFS revision."""
    # pylint: disable=too-many-return-statements,import-outside-toplevel
    if ONEFS_RELEASES['7.2.0.0'] <= revision < ONEFS_RELEASES['8.0.0.0']:
        import isi_sdk_7_2
        return isi_sdk_7_2
    if ONEFS_RELEASES['8.0.0.0'] <= revision < ONEFS_RELEASES['8.0.1.0']:
        import isi_sdk_8_0
        return isi_sdk_8_0
    if ONEFS_RELEASES['8.0.1.0'] <= revision < ONEFS_RELEASES['8.1.0.0']:
        import isi_sdk_8_0_1
        return isi_sdk_8_0_1
    if ONEFS_RELEASES['8.1.0.0'] <= revision < ONEFS_RELEASES['8.1.1.0']:
        import isi_sdk_8_1_0
        return isi_sdk_8_1_0
    if ONEFS_RELEASES['8.1.1.0'] <= revision < ONEFS_RELEASES['8.2.0.0']:
        import isi_sdk_8_1_1
        return isi_sdk_8_1_1
    if ONEFS_RELEASES['8.2.0.0'] <= revision < ONEFS_RELEASES['8.2.1.0']:
        import isi_sdk_8_2_0
        return isi_sdk_8_2_0
    if ONEFS_RELEASES['8.2.1.0'] <= revision < ONEFS_RELEASES['8.2.2.0']:
        import isi_sdk_8_2_1
        return isi_sdk_8_2_1
    if ONEFS_RELEASES['8.2.2.0'] <= revision < ONEFS_RELEASES['8.2.3.0']:
        import isi_sdk_8_2_2
        return isi_sdk_8_2_2
    # At this point, either the cluster is too old or too new;
    # however, new clusters still support old SDKs,
    # so, unless the caller asks to fail here, we'll fall back to the newest supported SDK.
    if strict:
        raise UnsupportedVersion('There is no SDK for OneFS revision {0}!'.format(hex(revision)))
    import isi_sdk_8_2_2
    return isi_sdk_8_2_2  # The latest SDK available.


def accesses_onefs(func):
    """Decorate a Client method that makes an SDK call directly."""
    def _decorated(self, *args, **kwargs):
        while True:
            try:
                return func(self, *args, **kwargs)
            except urllib3.exceptions.MaxRetryError as exc:
                if isinstance(exc.reason, urllib3.exceptions.SSLError):
                    # https://github.com/Isilon/isilon_sdk_python/issues/14
                    raise_from(OneFSCertificateError, exc)
                raise_from(OneFSConnectionError, exc)
            except self._sdk.rest.ApiException as exc:  # pylint: disable=protected-access
                if all([
                        # https://github.com/PyCQA/pylint/issues/2841
                        not exc.body,  # pylint: disable=no-member
                        'CERTIFICATE_VERIFY_FAILED' in (
                            exc.reason or ''  # pylint: disable=no-member
                        ),
                ]):
                    raise_from(OneFSCertificateError, exc)
                wrapped_exc = APIError(exc)
                if not wrapped_exc.try_again_error():
                    raise_from(wrapped_exc, exc)
                time.sleep(2)
                LOGGER.info(wrapped_exc.try_again_error_format)
    return _decorated


class OneFSValueError(OneFSError, ValueError):
    """
    This exception is raised by this module instead
    of a ValueError (but has the same meaning).
    """


def _license_is_active(license_):
    return license_.status.lower() in ['activated', 'evaluation', 'licensed']


class BaseClient(object):  # pylint: disable=too-many-public-methods,too-many-instance-attributes

    """Interact with OneFS."""

    def __init__(self, address, username, password, default_zone='System', verify_ssl=True):

        # Set attributes without setters first.
        self.default_zone = default_zone
        # We don't know what version we are pointed at yet, but we have to start somewhere.
        # Riptide was the first SDK to support ClusterApi().get_cluster_version.
        self._sdk = sdk_for_revision(ONEFS_RELEASES['8.0.0.0'])
        # Attributes with setters (see below) depend on having a Configuration object to manipulate.
        self._configuration = self._sdk.Configuration()
        self._address = None  # This will truly be set last.

        # Set attributes with setters last.
        self.verify_ssl = verify_ssl
        self.username = username
        self.password = password
        # Set the address last so the rest of the configuration is in place for making requests
        # (which will be needed for checking the cluster version).
        self.address = address

    @property
    def _api_client(self):
        return self._sdk.ApiClient(self._configuration)

    @accesses_onefs
    def _groups(self, zone=None):
        return self._sdk.AuthApi(self._api_client).list_auth_groups(
            zone=zone or self.default_zone,
        ).groups

    @accesses_onefs
    def _keytab_entries(self, provider):
        providers_krb5 = self._sdk.AuthApi(self._api_client).get_providers_krb5_by_id(provider)
        return providers_krb5.krb5[0].keytab_entries

    @accesses_onefs
    def _license(self, name):
        return self._sdk.LicenseApi(self._api_client).get_license_license(name)

    @accesses_onefs
    def _pools(self, *args, **kwargs):
        return self._sdk.NetworkApi(self._api_client).get_network_pools(*args, **kwargs).pools

    @accesses_onefs
    def _realms(self):
        return self._sdk.AuthApi(self._api_client).list_settings_krb5_realms().realm

    def _refresh_sdk(self):
        try:
            self._revision = self.revision()  # pylint: disable=attribute-defined-outside-init
        except AttributeError as exc:
            raise_from(UndeterminableVersion, exc)
        self._sdk = sdk_for_revision(self._revision)

    @accesses_onefs
    def _upgrade_cluster(self):
        return self._sdk.UpgradeApi(self._api_client).get_upgrade_cluster()

    @accesses_onefs
    def _version(self):
        return self._sdk.ClusterApi(self._api_client).get_cluster_version()

    def _zone(self, name):
        for zone in self._zones():
            # Zone names are NOT case-sensitive.
            if zone.name.lower() == name.lower():
                return zone
        raise MissingZoneError(name)

    def _zone_real_path(self, path, zone=None):
        return posixpath.join(
            self.zone_settings(zone=zone or self.default_zone)['path'],
            path.lstrip(posixpath.sep),
        )

    @accesses_onefs
    def _zones(self):
        return self._sdk.ZonesApi(self._api_client).list_zones().zones

    @accesses_onefs
    def acl_settings(self):
        """Get global ACL settings."""
        acl_settings = self._sdk.AuthApi(self._api_client).get_settings_acls().acl_policy_settings
        return {
            'access': acl_settings.access,
            'calcmode': acl_settings.calcmode,
            'calcmode_group': acl_settings.calcmode_group,
            'calcmode_owner': acl_settings.calcmode_owner,
            'chmod': acl_settings.chmod,
            'chmod_007': acl_settings.chmod_007,
            'chmod_inheritable': acl_settings.chmod_inheritable,
            'chown': acl_settings.chown,
            'create_over_smb': acl_settings.create_over_smb,
            'dos_attr': acl_settings.dos_attr,
            'group_owner_inheritance': acl_settings.group_owner_inheritance,
            'rwx': acl_settings.rwx,
            'synthetic_denies': acl_settings.synthetic_denies,
            'utimes': acl_settings.utimes,
        }

    @accesses_onefs
    def add_user_to_group(self, user_name, group_name, zone=None):
        """Add a user to a group."""
        group_member_cls = (
            self._sdk.GroupMember
            if self._revision < ONEFS_RELEASES['8.0.1.0'] else
            self._sdk.AuthAccessAccessItemFileGroup
        )
        try:
            self._sdk.AuthGroupsApi(self._api_client).create_group_member(
                group_member_cls(
                    type='user',
                    name=user_name,
                ),
                group_name,
                zone=zone or self.default_zone,
            )
        except ValueError as exc:
            # https://bugs.west.isilon.com/show_bug.cgi?id=231922
            assert all([
                str(exc) == 'Invalid value for `id`, must not be `None`',
                user_name in [
                    member.name
                    for member in self._sdk.AuthGroupsApi(self._api_client).list_group_members(
                        group_name,
                        zone=zone or self.default_zone,
                    ).members
                ],
            ])

    @property
    def address(self):
        """Get the address to connect to OneFS at."""
        # self._address may be None if self.host was set directly.
        return self._address or urlparse(self.host).hostname

    @address.setter
    def address(self, address):
        """
        Set the address to connect to OneFS at.
        If the address is a name, it will be resolved first to avoid config propagation problems.
        To avoid that, set host instead.
        """
        try:
            # If address is a SmartConnect name, making calls too fast can result in errors
            # due to changes not propagating fast enough across a cluster.
            # This problem gets worse on larger clusters.
            # So, we will choose 1 node to connect to and use that.
            netloc = socket.gethostbyname(address)
        except socket.gaierror as exc:
            raise_from(OneFSConnectionError, exc)
        if ':' in netloc:
            netloc = '[{ipv6}]'.format(ipv6=netloc)

        # Keep every part of self.host, except the hostname/address.
        parsed = urlparse(self.host)
        if parsed.port is not None:
            netloc += ':' + str(parsed.port)
        self.host = urlunparse(parsed._replace(netloc=netloc))

        # Setting self.host unsets self._address:
        self._address = address

    def check_license(self, name):
        """Check for a license on OneFS and raise a MissingLicenseError if it doesn't exist."""
        [license_] = self._license(name).licenses
        if not _license_is_active(license_):
            if license_.expiration and \
                    datetime.strptime(license_.expiration, '%Y-%m-%d').date() < date.today():
                raise ExpiredLicenseError(name)
            raise MissingLicenseError(name)

    def check_zone(self, name):
        """Check for a zone on OneFS and raise a MissingZoneError if it doesn't exist."""
        if not self.has_zone(name):
            raise MissingZoneError(name)

    @accesses_onefs
    def chmod(self, path, mode, zone=None):
        """Change the (integer) mode of a (zone-root-relative) path."""
        real_path = self._zone_real_path(path, zone=zone or self.default_zone)
        self._sdk.NamespaceApi(self._api_client).set_acl(
            namespace_path=real_path.lstrip(posixpath.sep),
            acl=True,
            namespace_acl=self._sdk.NamespaceAcl(
                authoritative='mode',
                mode='{:o}'.format(mode),
            ),
        )

    @accesses_onefs
    def chown(self, path, owner=None, group=None, zone=None):
        """Change the owning user and/or group of a (zone-root-relative) path."""
        real_path = self._zone_real_path(path, zone=zone)
        ns_acl_kwargs = {'authoritative': 'mode'}
        if owner is not None:
            # Get the UID of the owner to avoid name resolution problems across zones
            # (e.g. using the System zone to configure a different zone).
            ns_acl_kwargs['owner'] = self._sdk.MemberObject(
                type='UID',
                id='UID:{uid}'.format(
                    uid=owner if isinstance(owner, int) else self.uid_of_user(owner, zone=zone),
                ),
            )
        if group is not None:
            # Get the GID of the group to avoid name resolution problems across zones
            # (e.g. using the System zone to configure a different zone).
            ns_acl_kwargs['group'] = self._sdk.MemberObject(
                type='GID',
                id='GID:{gid}'.format(
                    gid=group if isinstance(group, int) else self.gid_of_group(group, zone=zone),
                ),
            )
        self._sdk.NamespaceApi(self._api_client).set_acl(
            namespace_path=real_path.lstrip(posixpath.sep),
            acl=True,
            namespace_acl=self._sdk.NamespaceAcl(**ns_acl_kwargs),
        )

    @accesses_onefs
    def create_auth_provider(self, realm, user, password):
        """Create a Kerberos auth provider."""
        self._sdk.AuthApi(self._api_client).create_providers_krb5_item(
            self._sdk.ProvidersKrb5Item(
                realm=realm,
                user=user,
                password=password,
            ),
        )

    @accesses_onefs
    def create_group(self, name, gid=None, zone=None):
        """Create a group."""
        self._sdk.AuthApi(self._api_client).create_auth_group(
            self._sdk.AuthGroupCreateParams(
                name=name,
                gid=gid,
            ),
            zone=zone or self.default_zone,
        )

    @accesses_onefs
    def create_hdfs_proxy_user(self, name, members=None, zone=None):
        """Create an HDFS proxy user."""
        if members is not None:
            group_member_cls = (
                self._sdk.GroupMember
                if self._revision < ONEFS_RELEASES['8.0.1.0'] else
                self._sdk.AuthAccessAccessItemFileGroup
            )
            members = [
                group_member_cls(
                    name=member_name,
                    type=member_type,
                )
                for member_name, member_type in members
            ]
        self._sdk.ProtocolsApi(self._api_client).create_hdfs_proxyuser(
            self._sdk.HdfsProxyuserCreateParams(name=name, members=members),
            zone=zone or self.default_zone,
        )

    @accesses_onefs
    def create_realm(self, name, admin_server, kdcs):
        """Create a realm configuration on OneFS."""
        try:
            self._sdk.AuthApi(self._api_client).create_settings_krb5_realm(
                self._sdk.SettingsKrb5RealmCreateParams(
                    realm=name,
                    admin_server=admin_server,
                    kdc=kdcs,
                ),
            )
        except ValueError as exc:
            # https://bugs.west.isilon.com/show_bug.cgi?id=231054
            auth_api = self._sdk.AuthApi(self._api_client)
            assert all([
                str(exc) == 'Invalid value for `id`, must not be `None`',
                name in [krb5.realm for krb5 in auth_api.list_settings_krb5_realms().realm],
            ])

    @accesses_onefs
    def create_spn(self, spn, realm, user, password):
        """Create an SPN in a Kerberos realm."""
        providers_krb5_item = self._sdk.ProvidersKrb5Item(
            realm=realm,
            user=user,
            password=password,
        )
        keytab_entry = self._sdk.ProvidersKrb5IdParamsKeytabEntry()
        keytab_entry.spn = spn
        providers_krb5_item.keytab_entries = [keytab_entry]
        self._sdk.AuthApi(self._api_client).create_providers_krb5_item(providers_krb5_item)

    @accesses_onefs
    def create_user(self, name, primary_group_name, uid=None, zone=None, enabled=None):
        """Create a user."""
        group_member_cls = (
            self._sdk.GroupMember
            if self._revision < ONEFS_RELEASES['8.0.1.0'] else
            self._sdk.AuthAccessAccessItemFileGroup
        )
        self._sdk.AuthApi(self._api_client).create_auth_user(
            self._sdk.AuthUserCreateParams(
                name=name,
                enabled=enabled,
                primary_group=group_member_cls(
                    type='group',
                    name=primary_group_name,
                ),
                uid=uid,
            ),
            zone=zone or self.default_zone,
        )

    @accesses_onefs
    def delete_auth_provider(self, name):
        """Delete a Kerberos auth provider."""
        self._sdk.AuthApi(self._api_client).delete_providers_krb5_by_id(name)

    @accesses_onefs
    def delete_group(self, name, zone=None):
        """Delete a group."""
        self._sdk.AuthApi(self._api_client).delete_auth_group(
            name,
            zone=zone or self.default_zone,
        )

    @accesses_onefs
    def delete_hdfs_proxy_user(
            self,
            name,
            zone=None,
    ):
        """Delete an HDFS proxy user."""
        self._sdk.ProtocolsApi(self._api_client).delete_hdfs_proxyuser(
            name,
            zone=zone or self.default_zone,
        )

    @accesses_onefs
    def delete_realm(self, name):
        """Delete a Kerberos realm configuration."""
        self._sdk.AuthApi(self._api_client).delete_settings_krb5_realm(name)

    @accesses_onefs
    def delete_spn(self, spn, provider):
        """Delete a Kerberos SPN."""
        self._sdk.AuthApi(self._api_client).update_providers_krb5_by_id(
            self._sdk.ProvidersKrb5IdParams(
                keytab_entries=[
                    keytab_entry
                    for keytab_entry in self._keytab_entries(provider=provider)
                    if keytab_entry.spn not in [spn, spn + '@' + provider]
                ]
            ),
            provider,
        )

    @accesses_onefs
    def delete_user(self, name, zone=None):
        """Delete a user."""
        self._sdk.AuthApi(self._api_client).delete_auth_user(
            name,
            zone=zone or self.default_zone,
        )

    def feature_is_supported(self, feature):
        """Determine if a given OneFSFeature is supported."""

        feature_gen, feature_bit = feature.value

        upgrade_cluster = self._upgrade_cluster()
        try:
            committed_features = upgrade_cluster.committed_features
        except AttributeError as exc:
            raise_from(
                UnsupportedOperation(
                    'OneFS 8.2.0 or later is required for feature flag support.',
                ),
                exc,
            )

        entries_for_gen = [
            entry.bits for entry in committed_features.gen_bits
            if entry.gen == feature_gen
        ]
        if not entries_for_gen:
            return bool(feature_gen <= committed_features.default_gen)

        return any(
            feature_bit == (i * 64) + offset  # Each entry can have up to 64 offsets.
            for i, offsets in enumerate(entries_for_gen)
            for offset in offsets
        )

    @accesses_onefs
    def flush_auth_cache(self, zone=None):
        """Flush the Security Objects Cache."""
        if self._revision < ONEFS_RELEASES['8.0.1.0']:
            _zone = zone or self.default_zone
            if _zone and _zone.lower() != 'system':
                raise UnsupportedOperation(
                    'The auth cache can only be flushed on the System zone before OneFS 8.0.1.',
                )
            response = requests.delete(
                url=self.host + '/platform/3/auth/users',
                verify=self.verify_ssl,
                auth=(self.username, self.password),
                params={'cached': True},
            )
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as exc:
                raise_from(NonSDKAPIError('The auth cache could not be flushed.'), exc)
            else:
                assert bool(
                    response.status_code == requests.codes.no_content,  # pylint: disable=no-member
                )
        else:
            try:
                self._sdk.AuthApi(self._api_client).create_auth_cache_item(
                    auth_cache_item=self._sdk.AuthCacheItem(all='all'),
                    zone=zone or self.default_zone,
                )
            except ValueError as exc:
                # https://bugs.west.isilon.com/show_bug.cgi?id=232142
                assert str(exc) == 'Invalid value for `id`, must not be `None`'

    @accesses_onefs
    def gid_of_group(self, group_name, zone=None):
        """Get the GID of a group."""
        auth_groups = self._sdk.AuthApi(self._api_client).get_auth_group(
            group_name,
            zone=zone or self.default_zone,
        )
        assert len(auth_groups.groups) == 1, 'Do you have duplicate groups (e.g. local and LDAP)?'
        return int(auth_groups.groups[0].gid.id.split(':')[1])

    def groups(self, zone=None):
        """Get the auth groups OneFS knows about."""
        for group in self._groups(zone=zone or self.default_zone):
            yield group.name

    def has_license(self, name):
        """Check for a OneFS license on OneFS."""
        return any(_license_is_active(license) for license in self._license(name).licenses)

    def has_zone(self, name):
        """Check for a zone on OneFS."""
        return self._zone(name) is not None

    @accesses_onefs
    def hdfs_inotify_settings(self, zone=None):
        """Get HDFS inotify settings for an access zone."""
        try:
            hdfs_inotify_settings = self._sdk.ProtocolsApi(
                self._api_client,
            ).get_hdfs_inotify_settings(
                zone=zone or self.default_zone,
            ).settings
        except AttributeError:
            raise UnsupportedOperation('OneFS 8.1.1 or later is required for INotify support.')
        return {
            'enabled': hdfs_inotify_settings.enabled,
            'maximum_delay': hdfs_inotify_settings.maximum_delay,
            'retention': hdfs_inotify_settings.retention,
        }

    @accesses_onefs
    def hdfs_settings(self, zone=None):
        """Get HDFS settings for an access zone."""
        hdfs_settings = self._sdk.ProtocolsApi(self._api_client).get_hdfs_settings(
            zone=zone or self.default_zone,
        ).settings
        return {
            'ambari_namenode': hdfs_settings.ambari_namenode,
            'ambari_server': hdfs_settings.ambari_server,
            'authentication_mode': hdfs_settings.authentication_mode,
            'default_block_size': hdfs_settings.default_block_size,
            'default_checksum_type': hdfs_settings.default_checksum_type,
            'odp_version': hdfs_settings.odp_version,
            'root_directory': hdfs_settings.root_directory,
            'service': hdfs_settings.service,
            'webhdfs_enabled': hdfs_settings.webhdfs_enabled,
        }

    @property
    def host(self):
        """Get the URL to connect to OneFS at."""
        return self._configuration.host

    @host.setter
    def host(self, host):
        """Set the URL to connect to OneFS at."""
        self._configuration.host = host
        # self.host may now point to an unrelated address:
        self._address = None
        # self.host may now point to a different version of OneFS:
        self._refresh_sdk()

    def list_spns(self, provider):
        """Get a list of keytab entries for a Kerberos auth provider."""
        return [keytab_entry.spn for keytab_entry in self._keytab_entries(provider=provider)]

    @accesses_onefs
    def mkdir(self, path, mode, recursive=False, overwrite=False, zone=None):
        """Create a directory at a (zone-root-relative) path with the given (integer) mode."""
        real_path = self._zone_real_path(path, zone=zone or self.default_zone)
        if posixpath.sep not in real_path.strip(posixpath.sep):
            # The first component of real_path is actually a RAN namespace.
            # In this case, there is only one component: ifs.
            # The ifs namespace cannot be modified,
            # but calling create_directory on any namespace will fail.
            raise OneFSValueError('Calling mkdir on the ifs namespace will fail.')
        self._sdk.NamespaceApi(self._api_client).create_directory(
            directory_path=real_path.lstrip(posixpath.sep),
            x_isi_ifs_target_type='container',
            x_isi_ifs_access_control='{:o}'.format(mode),
            recursive=recursive,
            overwrite=overwrite,
        )

    @accesses_onefs
    def node_addresses(self, zone=None):
        """Get IP addresses in pools associated with a zone."""
        return {
            socket.inet_ntoa(struct.pack("!I", ip))
            for pool in self._sdk.NetworkApi(self._api_client).get_network_pools(
                access_zone=zone or self.default_zone,
            ).pools
            for range_ in pool.ranges
            for ip in range(
                struct.unpack("!I", socket.inet_aton(range_.low))[0],
                struct.unpack("!I", socket.inet_aton(range_.high))[0] + 1,
            )
        }

    @property
    def password(self):
        """Get the password to connect to OneFS with."""
        return self._configuration.password

    @password.setter
    def password(self, password):
        """Set the password to connect to OneFS with."""
        self._configuration.password = password

    @accesses_onefs
    def permissions(self, path, zone=None):
        """Get the owner, group, and (integer) mode of a (zone-root-relative) path."""
        real_path = self._zone_real_path(path, zone=zone or self.default_zone)
        acl = self._sdk.NamespaceApi(self._api_client).get_acl(
            namespace_path=real_path.lstrip(posixpath.sep),
            acl=True,
        )
        return {
            'group': acl.group.name,
            'mode': int(acl.mode, base=8),
            'owner': acl.owner.name,
        }

    @accesses_onefs
    def primary_group_of_user(self, user_name, zone=None):
        """Get the name of the primary group of a user."""
        auth_users = self._sdk.AuthApi(self._api_client).get_auth_user(
            user_name,
            zone=zone or self.default_zone,
        )
        assert len(auth_users.users) == 1, 'Do you have duplicate users (e.g. local and LDAP)?'
        return auth_users.users[0].gid.name

    def realms(self):
        """Get the Kerberos realms OneFS knows about."""
        for realm in self._realms():
            yield realm.realm

    def revision(self):
        """Get the revision number of the cluster."""
        revisions = set(self.revisions().values())
        if len(revisions) != 1:
            raise MixedModeError(', '.join(revisions))
        return revisions.pop()

    def revisions(self):
        """Get the revision numbers of each node in the cluster."""
        return {node.id: int(node.revision) for node in self._version().nodes}

    @accesses_onefs
    def rmdir(self, path, recursive=False, zone=None):
        """Delete the directory at a (zone-root-relative) path."""
        real_path = self._zone_real_path(path, zone=zone or self.default_zone)
        self._sdk.NamespaceApi(self._api_client).delete_directory(
            directory_path=real_path.lstrip(posixpath.sep),
            recursive=recursive,
        )

    def smartconnect_zone(self, smartconnect):
        """Get the access zone name associated with a SmartConnect name."""
        for pool in self._pools():
            if pool.sc_dns_zone.lower() == smartconnect.lower():
                return pool.access_zone
        return None

    @accesses_onefs
    def uid_of_user(self, user_name, zone=None):
        """Get the UID of a user."""
        auth_users = self._sdk.AuthApi(self._api_client).get_auth_user(
            user_name,
            zone=zone or self.default_zone,
        )
        assert len(auth_users.users) == 1, 'Do you have duplicate users (e.g. local and LDAP)?'
        return int(auth_users.users[0].uid.id.split(':')[1])

    @accesses_onefs
    def update_acl_settings(self, settings):
        """Set ACL settings."""
        acl_settings = self._sdk.SettingsAclsAclPolicySettings()
        for key, value in settings.items():
            try:
                getattr(acl_settings, key)
            except AttributeError as exc:
                raise_from(OneFSValueError('"{0}" is not a valid ACL setting.'.format(key)), exc)
            setattr(acl_settings, key, value)
        self._sdk.AuthApi(self._api_client).update_settings_acls(acl_settings)

    @accesses_onefs
    def update_hdfs_settings(self, settings, zone=None):
        """Set HDFS settings for an access zone."""
        hdfs_settings = self._sdk.HdfsSettingsSettings()
        for key, value in settings.items():
            try:
                getattr(hdfs_settings, key)
            except AttributeError as exc:
                raise_from(OneFSValueError('"{0}" is not a valid HDFS setting.'.format(key)), exc)
            setattr(hdfs_settings, key, value)
        self._sdk.ProtocolsApi(self._api_client).update_hdfs_settings(
            hdfs_settings,
            zone=zone or self.default_zone,
        )

    @accesses_onefs
    def update_zone_settings(self, settings, zone=None):
        """Set the settings for an access zone."""
        zone_settings = self._sdk.Zone()
        for key, value in settings.items():
            try:
                getattr(zone_settings, key)
            except AttributeError as exc:
                raise_from(OneFSValueError('"{0}" is not a valid zone setting.'.format(key)), exc)
            setattr(zone_settings, key, value)
        self._sdk.ZonesApi(self._api_client).update_zone(zone_settings, zone or self.default_zone)

    @property
    def username(self):
        """Get the user to connect to OneFS as."""
        return self._configuration.username

    @username.setter
    def username(self, username):
        """Set the user to connect to OneFS as."""
        self._configuration.username = username

    @accesses_onefs
    def user_groups(self, user_name, zone=None):
        """Get the groups a user is in."""
        auth_users = self._sdk.AuthApi(self._api_client).get_auth_user(
            auth_user_id='USER:{name}'.format(name=user_name),
            query_member_of=True,
            zone=zone or self.default_zone,
        )
        assert len(auth_users.users) == 1, 'Do you have duplicate users (e.g. local and LDAP)?'
        return [group.name for group in auth_users.users[0].member_of]

    @accesses_onefs
    def users(
            self,
            zone=None,
            key=lambda sdk_auth_user: sdk_auth_user.name,
            filter_=lambda _: True,
    ):
        """Get a list of users that exist in an access zone on OneFS."""
        for user in self._sdk.AuthApi(self._api_client).list_auth_users(
                zone=zone or self.default_zone,
        ).users:
            if filter_(user):
                yield key(user)

    @property
    def verify_ssl(self):
        """Determine whether the OneFS SSL certificate will be verified or not."""
        return self._configuration.verify_ssl

    @verify_ssl.setter
    def verify_ssl(self, verify_ssl):
        """Specify whether to verify the OneFS SSL certificate or not."""
        self._configuration.verify_ssl = bool(verify_ssl)

    @property
    def zone(self):
        """Get the default zone (used when a zone is not provided for a zone-specific operation)."""
        return self.default_zone

    def zone_settings(self, zone=None):
        """Get settings for an access zone."""
        zone_settings = self._zone(zone or self.default_zone)
        return {
            'alternate_system_provider': zone_settings.alternate_system_provider,
            'auth_providers': zone_settings.auth_providers,
            'cache_entry_expiry': zone_settings.cache_entry_expiry,
            'create_path': zone_settings.create_path,
            'groupnet': zone_settings.groupnet,
            'home_directory_umask': zone_settings.home_directory_umask,
            'id': zone_settings.id,
            'map_untrusted': zone_settings.map_untrusted,
            'name': zone_settings.name,
            'netbios_name': zone_settings.netbios_name,
            'path': zone_settings.path,
            'skeleton_directory': zone_settings.skeleton_directory,
            'system': zone_settings.system,
            'system_provider': zone_settings.system_provider,
            'user_mapping_rules': zone_settings.user_mapping_rules,
            'zone_id': zone_settings.zone_id,
        }

    def zones(self):
        """Get the list of access zone names available on the cluster."""
        for zone in self._zones():
            yield zone.name


class Client(BaseClient):

    """Do some basic checks after connecting to OneFS."""

    @classmethod
    def for_hdfs(cls, *args, **kwargs):
        """Connect to OneFS and do some basic HDFS-related checks."""
        onefs = cls(*args, **kwargs)
        onefs.check_license('HDFS')
        LOGGER.debug('HDFS is licensed.')
        return onefs

    def __init__(self, *args, **kwargs):
        LOGGER.debug('Connecting to the OneFS cluster...')
        super(Client, self).__init__(*args, **kwargs)
        LOGGER.debug('OneFS interactions will go to %s.', self.host)
        self.check_zone(self.zone)
        LOGGER.debug('The %s zone exists.', self.zone)
