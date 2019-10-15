"""Define and create necessary Hadoop users and groups on OneFS."""

from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os

import isilon_hadoop_tools.onefs


__all__ = [
    # Functions
    'cdh_identities',
    'hdp_identities',
    'iterate_identities',
    'log_identities',
    'with_suffix_applied',

    # Objects
    'Creator',
]

LOGGER = logging.getLogger(__name__)


def _log_create_group(group_name):
    LOGGER.info('Create %s group.', group_name)


def _log_create_user(user_name, pgroup_name):
    LOGGER.info('Create %s:%s user.', user_name, pgroup_name)


def _log_add_user_to_group(user_name, group_name):
    LOGGER.info('Add %s user to %s group.', user_name, group_name)


def _log_create_proxy_user(proxy_user_name, members):
    LOGGER.info(
        'Create %s proxy user with the following members: %s.',
        proxy_user_name,
        ', '.join(
            '{0} ({1})'.format(member_name, member_type)
            for member_name, member_type in members
        ),
    )


class Creator(object):

    """
    Create users and groups with contiguous IDs on OneFS
    and in a local user/group creation script for Linux.
    """
    # pylint: disable=logging-format-interpolation

    default_start_uid = 1025
    default_start_gid = 1025

    def __init__(  # pylint: disable=too-many-arguments
            self,
            onefs,
            onefs_zone=None,
            start_uid=default_start_uid,
            start_gid=default_start_gid,
            script_path=None,
    ):
        self.onefs = onefs
        self.onefs_zone = onefs_zone
        self._next_uid = start_uid
        self._next_gid = start_gid
        self.script_path = script_path

    @property
    def next_gid(self):
        """Get the next monotonically-increasing GID (begins at start_gid)."""
        try:
            return self._next_gid
        finally:
            self._next_gid += 1

    @property
    def next_uid(self):
        """Get the next monotonically-increasing UID (begins at start_uid)."""
        try:
            return self._next_uid
        finally:
            self._next_uid += 1

    def add_user_to_group(self, user_name, group_name):
        """Add a user to a group on OneFS and in the local group-creation script."""
        try:
            LOGGER.info(
                'Adding the %s user to the %s group on %s...',
                user_name,
                group_name,
                self.onefs.address,
            )
            self.onefs.add_user_to_group(
                user_name=user_name,
                group_name=group_name,
                zone=self.onefs_zone,
            )
        except isilon_hadoop_tools.onefs.APIError as exc:
            uid = self.onefs.uid_of_user(user_name=user_name, zone=self.onefs_zone)
            if exc.user_already_in_group_error(uid, group_name):
                LOGGER.warning(exc.user_already_in_group_error_format.format(uid, group_name))
            else:
                raise
        if self.script_path:
            self._create_script()
            LOGGER.info(
                'Adding the %s user to the %s group in %s...',
                user_name,
                group_name,
                self.script_path,
            )
            with open(self.script_path, 'a') as script_file:
                script_file.write(
                    'usermod -a -G {group} {user}\n'.format(group=group_name, user=user_name),
                )

    def create_group(self, group_name):
        """Create a group on OneFS and in the local script."""
        while True:
            try:
                gid = self.next_gid
                LOGGER.info(
                    'Creating the %s group with GID %s on %s...',
                    group_name,
                    gid,
                    self.onefs.address,
                )
                self.onefs.create_group(name=group_name, gid=gid, zone=self.onefs_zone)
                break
            except isilon_hadoop_tools.onefs.APIError as exc:
                if exc.gid_already_exists_error(gid):
                    LOGGER.warning(exc.gid_already_exists_error_format.format(gid))
                    continue
                if exc.group_already_exists_error(group_name):
                    LOGGER.warning(exc.group_already_exists_error_format.format(group_name))
                    gid = self.onefs.gid_of_group(group_name=group_name, zone=self.onefs_zone)
                    break
                raise
        if self.script_path:
            self._create_script()
            LOGGER.info(
                'Creating the %s group with GID %s in %s...',
                group_name,
                gid,
                self.script_path,
            )
            with open(self.script_path, 'a') as script_file:
                script_file.write('groupadd --gid {gid} {name}\n'.format(gid=gid, name=group_name))
        return gid

    def create_identities(
            self,
            identities,
            create_group=None,
            create_user=None,
            add_user_to_group=None,
            create_proxy_user=None,
            _flush_auth_cache=None,
            _create_script=None,
    ):
        """Create identities on OneFS and in the local script."""
        if self.onefs_zone.lower() == 'system':
            LOGGER.warning('Deploying in the System zone is not recommended.')
        if self.script_path:
            LOGGER.info('Creating %s...', self.script_path)
            (_create_script or self._create_script)()
        iterate_identities(
            identities,
            create_group=create_group or self.create_group,
            create_user=create_user or self.create_user,
            add_user_to_group=add_user_to_group or self.add_user_to_group,
            create_proxy_user=create_proxy_user or self.create_proxy_user,
        )
        LOGGER.info('Flushing the auth cache...')
        (_flush_auth_cache or self.onefs.flush_auth_cache)()

    def log_identities(self, identities):
        """Log the actions that would be taken by create_identities."""
        self.create_identities(
            identities,
            create_group=_log_create_group,
            create_user=_log_create_user,
            add_user_to_group=_log_add_user_to_group,
            create_proxy_user=_log_create_proxy_user,
            _flush_auth_cache=lambda: None,
            _create_script=lambda: None,
        )

    def create_proxy_user(self, proxy_user_name, members):
        """Create a proxy user on OneFS."""
        try:
            LOGGER.info(
                'Creating the %s proxy user with the following members: %s...',
                proxy_user_name,
                ', '.join(
                    '{0} ({1})'.format(member_name, member_type)
                    for member_name, member_type in members
                ),
            )
            self.onefs.create_hdfs_proxy_user(
                name=proxy_user_name,
                members=members,
                zone=self.onefs_zone,
            )
        except isilon_hadoop_tools.onefs.APIError as exc:
            if exc.proxy_user_already_exists_error(proxy_user_name=proxy_user_name):
                LOGGER.warning(exc.proxy_user_already_exists_error_format.format(proxy_user_name))
                return
            raise

    def _create_script(self):
        if not os.path.exists(self.script_path):
            with open(self.script_path, 'w') as script_file:
                script_file.write('#!/usr/bin/env sh\n')
                script_file.write('set -o errexit\n')
                script_file.write('set -o xtrace\n')

    def create_user(self, user_name, primary_group_name):
        """Create a user on OneFS and in the local script."""
        while True:
            try:
                uid = self.next_uid
                LOGGER.info(
                    'Creating the %s user with UID %s on %s...',
                    user_name,
                    uid,
                    self.onefs.address,
                )
                self.onefs.create_user(
                    name=user_name,
                    uid=uid,
                    primary_group_name=primary_group_name,
                    zone=self.onefs_zone,
                    enabled=True,
                )
                break
            except isilon_hadoop_tools.onefs.APIError as exc:
                if exc.uid_already_exists_error(uid):
                    LOGGER.warning(exc.uid_already_exists_error_format.format(uid))
                    continue
                if exc.user_already_exists_error(user_name):
                    LOGGER.warning(exc.user_already_exists_error_format.format(user_name))
                    uid = self.onefs.uid_of_user(user_name=user_name, zone=self.onefs_zone)
                    break
                raise
        if self.script_path:
            self._create_script()
            LOGGER.info(
                'Creating the %s user with UID %s in %s...',
                user_name,
                uid,
                self.script_path,
            )
            with open(self.script_path, 'a') as script_file:
                script_file.write(
                    'useradd --uid {uid} --gid {gid} {name}\n'.format(
                        uid=uid,
                        gid=self.onefs.gid_of_group(
                            group_name=self.onefs.primary_group_of_user(
                                user_name=user_name,
                                zone=self.onefs_zone,
                            ),
                            zone=self.onefs_zone,
                        ),
                        name=user_name,
                    ),
                )
        return uid


def iterate_identities(
        identities,
        create_group,
        create_user,
        add_user_to_group,
        create_proxy_user,
):
    """Iterate over all groups, users, and proxy users in creation-order."""

    created_group_names = set()
    for group_name in identities['groups']:
        if group_name not in created_group_names:
            create_group(group_name)
            created_group_names.add(group_name)

    for user_name, (pgroup_name, sgroup_names) in identities['users'].items():
        for group_name in sgroup_names.union({pgroup_name}):
            if group_name not in created_group_names:
                create_group(group_name)
                created_group_names.add(group_name)
        create_user(user_name, pgroup_name)
        for group_name in sgroup_names:
            add_user_to_group(user_name, group_name)

    for proxy_user_name, members in identities['proxy_users'].items():
        create_proxy_user(proxy_user_name, members)


def log_identities(identities):
    """Iterate identities in creation-order and log the actions that would be taken."""
    iterate_identities(
        identities,
        create_group=_log_create_group,
        create_user=_log_create_user,
        add_user_to_group=_log_add_user_to_group,
        create_proxy_user=_log_create_proxy_user,
    )


def with_suffix_applied(
        identities,
        suffix,
        applicator=lambda identity, suffix: identity + suffix,
):
    """Append a suffix to all identities."""
    return {
        'groups': {applicator(group_name, suffix) for group_name in identities['groups']},
        'users': {
            applicator(user_name, suffix): (
                applicator(pgroup_name, suffix),
                {applicator(sgroup_name, suffix) for sgroup_name in sgroup_names},
            )
            for user_name, (pgroup_name, sgroup_names) in identities['users'].items()
        },
        'proxy_users': {
            applicator(proxy_user_name, suffix): {
                (applicator(member_name, suffix), member_type)
                for member_name, member_type in members
            }
            for proxy_user_name, members in identities['proxy_users'].items()
        }
    }


def cdh_identities(zone):
    """Identities needed for Cloudera Distribution including Hadoop"""
    smoke_user = ('cloudera-scm', 'user')
    identities = {
        'groups': set(),  # Groups with no users in them.
        'users': {
            'accumulo': ('accumulo', set()),
            'anonymous': ('anonymous', set()),
            'apache': ('apache', set()),
            'cloudera-scm': ('cloudera-scm', set()),
            'cmjobuser': ('cmjobuser', set()),
            'flume': ('flume', set()),
            'hbase': ('hbase', {'hadoop', 'supergroup'}),
            'hdfs': ('hdfs', {'hadoop', 'supergroup'}),
            'hive': ('hive', set()),
            'HTTP': ('HTTP', {'hadoop', 'supergroup'}),
            'httpfs': ('httpfs', set()),
            'hue': ('hue', set()),
            'impala': ('impala', {'hive'}),
            'kafka': ('kafka', set()),
            'keytrustee': ('keytrustee', set()),
            'kms': ('kms', set()),
            'kudu': ('kudu', set()),
            'llama': ('llama', set()),
            'mapred': ('mapred', {'hadoop', 'supergroup'}),
            'oozie': ('oozie', set()),
            'sentry': ('sentry', set()),
            'solr': ('solr', set()),
            'spark': ('spark', set()),
            'sqoop': ('sqoop', {'sqoop2'}),
            'sqoop2': ('sqoop2', {'sqoop'}),
            'yarn': ('yarn', {'hadoop', 'supergroup'}),
            'zookeeper': ('zookeeper', set()),
        },
        'proxy_users': {
            'flume': {smoke_user, ('hadoop', 'group')},
            'hive': {smoke_user, ('hadoop', 'group')},
            'HTTP': {smoke_user},
            'hue': {smoke_user, ('hadoop', 'group')},
            'impala': {smoke_user, ('hadoop', 'group')},
            'mapred': {smoke_user, ('hadoop', 'group')},
            'oozie': {smoke_user, ('hadoop', 'group')},
        },
    }
    if zone.lower() != 'system':
        identities['users']['admin'] = ('admin', set())
    return identities


def hdp_identities(zone):
    """Identities needed for Hortonworks Data Platform"""
    smoke_user = ('ambari-qa', 'user')
    identities = {
        'groups': set(),  # Groups with no users in them.
        'users': {
            'accumulo': ('accumulo', {'hadoop'}),
            'activity_analyzer': ('activity_analyzer', {'hadoop'}),
            'activity_explorer': ('activity_explorer', {'hadoop'}),
            'ambari-qa': ('ambari-qa', {'hadoop'}),
            'ambari-server': ('ambari-server', {'hadoop'}),
            'ams': ('ams', {'hadoop'}),
            'anonymous': ('anonymous', set()),
            'atlas': ('atlas', {'hadoop'}),
            'druid': ('druid', {'hadoop'}),
            'falcon': ('falcon', {'hadoop'}),
            'flume': ('flume', {'hadoop'}),
            'gpadmin': ('gpadmin', {'hadoop'}),
            'hadoopqa': ('hadoopqa', {'hadoop'}),
            'hbase': ('hbase', {'hadoop'}),
            'hcat': ('hcat', {'hadoop'}),
            'hdfs': ('hdfs', {'hadoop'}),
            'hive': ('hive', {'hadoop'}),
            'HTTP': ('HTTP', {'hadoop'}),
            'hue': ('hue', {'hadoop'}),
            'infra-solr': ('infra-solr', {'hadoop'}),
            'kafka': ('kafka', {'hadoop'}),
            'keyadmin': ('keyadmin', {'hadoop'}),
            'kms': ('kms', {'hadoop'}),
            'knox': ('knox', {'hadoop'}),
            'livy': ('livy', {'hadoop'}),
            'logsearch': ('logsearch', {'hadoop'}),
            'mahout': ('mahout', {'hadoop'}),
            'mapred': ('mapred', {'hadoop'}),
            'oozie': ('oozie', {'hadoop'}),
            'ranger': ('ranger', {'hadoop'}),
            'rangerlookup': ('rangerlookup', {'hadoop'}),
            'spark': ('spark', {'hadoop'}),
            'sqoop': ('sqoop', {'hadoop'}),
            'storm': ('storm', {'hadoop'}),
            'tez': ('tez', {'hadoop'}),
            'tracer': ('tracer', {'hadoop'}),
            'yarn': ('yarn', {'hadoop'}),
            'yarn-ats': ('yarn-ats', {'hadoop'}),
            'yarn-ats-hbase': ('yarn-ats-hbase', {'hadoop'}),
            'zeppelin': ('zeppelin', {'hadoop'}),
            'zookeeper': ('zookeeper', {'hadoop'}),
        },
        'proxy_users': {
            'ambari-server': {smoke_user},
            'flume': {smoke_user, ('hadoop', 'group')},
            'hbase': {smoke_user, ('hadoop', 'group')},
            'hcat': {smoke_user, ('hadoop', 'group')},
            'hive': {smoke_user, ('hadoop', 'group')},
            'HTTP': {smoke_user},
            'knox': {smoke_user},
            'livy': {smoke_user, ('hadoop', 'group')},
            'oozie': {smoke_user, ('hadoop', 'group')},
            'yarn': {smoke_user, ('hadoop', 'group')},
        },
    }
    if zone.lower() != 'system':
        identities['users']['admin'] = ('admin', set())
    return identities
