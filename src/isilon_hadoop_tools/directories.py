"""Define and create directories with appropriate permissions on OneFS."""

from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import posixpath

import isilon_hadoop_tools.onefs
from isilon_hadoop_tools import IsilonHadoopToolError

__all__ = [
    # Exceptions
    'DirectoriesError',
    'HDFSRootDirectoryError',

    # Functions
    'cdh_directories',
    'hdp_directories',

    # Objects
    'Creator',
    'HDFSDirectory',
]

LOGGER = logging.getLogger(__name__)


class DirectoriesError(IsilonHadoopToolError):
    """All exceptions emitted from this module inherit from this Exception."""


class HDFSRootDirectoryError(DirectoriesError):
    """This exception occurs when the HDFS root directory is not set to a usable path."""


class Creator(object):

    """Create directories with appropriate ownership and permissions on OneFS."""

    def __init__(self, onefs, onefs_zone=None):
        self.onefs = onefs
        self.onefs_zone = onefs_zone

    def create_directories(self, directories, setup=None, mkdir=None, chmod=None, chown=None):
        """Create directories on HDFS on OneFS."""
        if self.onefs_zone.lower() == 'system':
            LOGGER.warning('Deploying in the System zone is not recommended.')
        sep = posixpath.sep
        zone_root = self.onefs.zone_settings(zone=self.onefs_zone)['path'].rstrip(sep)
        hdfs_root = self.onefs.hdfs_settings(zone=self.onefs_zone)['root_directory'].rstrip(sep)
        if hdfs_root == zone_root:
            LOGGER.warning('The HDFS root is the same as the zone root.')
        if hdfs_root == '/ifs':
            # The HDFS root requires non-default ownership/permissions,
            # and modifying /ifs can break NFS/SMB.
            raise HDFSRootDirectoryError(hdfs_root)
        assert hdfs_root.startswith(zone_root)
        zone_hdfs = hdfs_root[len(zone_root):]
        if setup:
            setup(zone_root, hdfs_root, zone_hdfs)
        for directory in directories:
            path = posixpath.join(zone_hdfs, directory.path.lstrip(posixpath.sep))
            LOGGER.info("mkdir '%s%s'", zone_root, path)
            try:
                (mkdir or self.onefs.mkdir)(path, directory.mode, zone=self.onefs_zone)
            except isilon_hadoop_tools.onefs.APIError as exc:
                if exc.dir_path_already_exists_error():
                    LOGGER.warning("%s%s already exists. ", zone_root, path)
                else:
                    raise
            LOGGER.info("chmod '%o' '%s%s'", directory.mode, zone_root, path)
            (chmod or self.onefs.chmod)(path, directory.mode, zone=self.onefs_zone)
            LOGGER.info("chown '%s:%s' '%s%s'", directory.owner, directory.group, zone_root, path)
            (chown or self.onefs.chown)(
                path,
                owner=directory.owner,
                group=directory.group,
                zone=self.onefs_zone,
            )

    def log_directories(self, directories):
        """Log the actions that would be taken by create_directories."""
        def _pass(*_, **__):
            pass
        self.create_directories(directories, setup=_pass, mkdir=_pass, chmod=_pass, chown=_pass)


class HDFSDirectory(object):  # pylint: disable=too-few-public-methods

    """A Directory on HDFS"""

    def __init__(self, path, owner, group, mode):
        self.path = path
        self.owner = owner
        self.group = group
        self.mode = mode

    def apply_identity_suffix(self, suffix):
        """Append a suffix to all identities associated with the directory."""
        self.owner += suffix
        self.group += suffix


def cdh_directories(identity_suffix=None):
    """Directories needed for Cloudera Distribution including Hadoop"""
    directories = [
        HDFSDirectory('/', 'hdfs', 'hadoop', 0o755),
        HDFSDirectory('/hbase', 'hbase', 'hbase', 0o755),
        HDFSDirectory('/solr', 'solr', 'solr', 0o775),
        HDFSDirectory('/tmp', 'hdfs', 'supergroup', 0o1777),
        HDFSDirectory('/tmp/hive', 'hive', 'supergroup', 0o777),
        HDFSDirectory('/tmp/logs', 'mapred', 'hadoop', 0o1777),
        HDFSDirectory('/user', 'hdfs', 'supergroup', 0o755),
        HDFSDirectory('/user/flume', 'flume', 'flume', 0o775),
        HDFSDirectory('/user/hdfs', 'hdfs', 'hdfs', 0o755),
        HDFSDirectory('/user/history', 'mapred', 'hadoop', 0o777),
        HDFSDirectory('/user/hive', 'hive', 'hive', 0o775),
        HDFSDirectory('/user/hive/warehouse', 'hive', 'hive', 0o1777),
        HDFSDirectory('/user/hue', 'hue', 'hue', 0o755),
        HDFSDirectory('/user/hue/.cloudera_manager_hive_metastore_canary', 'hue', 'hue', 0o777),
        HDFSDirectory('/user/impala', 'impala', 'impala', 0o775),
        HDFSDirectory('/user/oozie', 'oozie', 'oozie', 0o775),
        HDFSDirectory('/user/spark', 'spark', 'spark', 0o751),
        HDFSDirectory('/user/spark/applicationHistory', 'spark', 'spark', 0o1777),
        HDFSDirectory('/user/sqoop2', 'sqoop2', 'sqoop', 0o775),
        HDFSDirectory('/user/yarn', 'yarn', 'yarn', 0o755),
    ]
    if identity_suffix:
        for directory in directories:
            directory.apply_identity_suffix(identity_suffix)
    return directories


def hdp_directories(identity_suffix=None):
    """Directories needed for Hortonworks Data Platform"""
    directories = [
        HDFSDirectory('/', 'hdfs', 'hadoop', 0o755),
        HDFSDirectory('/app-logs', 'yarn', 'hadoop', 0o1777),
        HDFSDirectory('/app-logs/ambari-qa', 'ambari-qa', 'hadoop', 0o770),
        HDFSDirectory('/app-logs/ambari-qa/logs', 'ambari-qa', 'hadoop', 0o770),
        HDFSDirectory('/apps', 'hdfs', 'hadoop', 0o755),
        HDFSDirectory('/apps/accumulo', 'accumulo', 'hadoop', 0o750),
        HDFSDirectory('/apps/falcon', 'falcon', 'hdfs', 0o777),
        HDFSDirectory('/apps/hbase', 'hdfs', 'hadoop', 0o755),
        HDFSDirectory('/apps/hbase/data', 'hbase', 'hadoop', 0o775),
        HDFSDirectory('/apps/hbase/staging', 'hbase', 'hadoop', 0o711),
        HDFSDirectory('/apps/hive', 'hdfs', 'hdfs', 0o755),
        HDFSDirectory('/apps/hive/warehouse', 'hive', 'hdfs', 0o777),
        HDFSDirectory('/apps/tez', 'tez', 'hdfs', 0o755),
        HDFSDirectory('/apps/webhcat', 'hcat', 'hdfs', 0o755),
        HDFSDirectory('/ats', 'yarn', 'hdfs', 0o755),
        HDFSDirectory('/ats/done', 'yarn', 'hdfs', 0o775),
        HDFSDirectory('/atsv2', 'yarn-ats', 'hadoop', 0o755),
        HDFSDirectory('/mapred', 'mapred', 'hadoop', 0o755),
        HDFSDirectory('/mapred/system', 'mapred', 'hadoop', 0o755),
        HDFSDirectory('/system', 'yarn', 'hadoop', 0o755),
        HDFSDirectory('/system/yarn', 'yarn', 'hadoop', 0o755),
        HDFSDirectory('/system/yarn/node-labels', 'yarn', 'hadoop', 0o700),
        HDFSDirectory('/tmp', 'hdfs', 'hdfs', 0o1777),
        HDFSDirectory('/tmp/hive', 'ambari-qa', 'hdfs', 0o777),
        HDFSDirectory('/user', 'hdfs', 'hdfs', 0o755),
        HDFSDirectory('/user/ambari-qa', 'ambari-qa', 'hdfs', 0o770),
        HDFSDirectory('/user/hcat', 'hcat', 'hdfs', 0o755),
        HDFSDirectory('/user/hdfs', 'hdfs', 'hdfs', 0o755),
        HDFSDirectory('/user/hive', 'hive', 'hdfs', 0o700),
        HDFSDirectory('/user/hue', 'hue', 'hue', 0o755),
        HDFSDirectory('/user/oozie', 'oozie', 'hdfs', 0o775),
        HDFSDirectory('/user/yarn', 'yarn', 'hdfs', 0o755),
    ]
    if identity_suffix:
        for directory in directories:
            directory.apply_identity_suffix(identity_suffix)
    return directories
