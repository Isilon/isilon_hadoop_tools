"""Command-line interface for entry points"""

from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
import sys
import time

from future.utils import raise_from
import urllib3

import isilon_hadoop_tools
import isilon_hadoop_tools.cli
import isilon_hadoop_tools.directories
import isilon_hadoop_tools.identities


DRY_RUN = 'Had this been for real, this is what would have happened...'
LOGGER = logging.getLogger(__name__)


def base_cli(parser=None):
    """Define CLI arguments and options for all entry points."""
    if parser is None:
        parser = isilon_hadoop_tools.cli.base_cli()
    parser.add_argument(
        '--append-cluster-name',
        help='the cluster name to append on identities',
        type=str,
    )
    parser.add_argument(
        '--dist',
        help='the Hadoop distribution to be deployed',
        choices=('cdh', 'hdp'),
        required=True,
    )
    parser.add_argument(
        '--dry',
        help='do a dry run (only logs)',
        action='store_true',
        default=False,
    )
    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s v{0}'.format(isilon_hadoop_tools.__version__),
    )
    return parser


def configure_script(args):
    """Logic that applies to all scripts goes here."""
    if args.no_verify:
        urllib3.disable_warnings()


def isilon_create_users_cli(parser=None):
    """Define CLI arguments and options for isilon_create_users."""
    if parser is None:
        parser = base_cli()
    parser.add_argument(
        '--start-gid',
        help='the lowest GID to create a group with',
        type=int,
        default=isilon_hadoop_tools.identities.Creator.default_start_gid,
    )
    parser.add_argument(
        '--start-uid',
        help='the lowest UID to create a user with',
        type=int,
        default=isilon_hadoop_tools.identities.Creator.default_start_uid,
    )
    return parser


@isilon_hadoop_tools.cli.catches(isilon_hadoop_tools.IsilonHadoopToolError)
def isilon_create_users(argv=None):
    """Execute isilon_create_users commands."""

    if argv is None:
        argv = sys.argv[1:]
    args = isilon_create_users_cli().parse_args(argv)

    isilon_hadoop_tools.cli.configure_logging(args)
    configure_script(args)
    onefs = isilon_hadoop_tools.cli.hdfs_client(args)

    identities = {
        'cdh': isilon_hadoop_tools.identities.cdh_identities,
        'hdp': isilon_hadoop_tools.identities.hdp_identities,
    }[args.dist](args.zone)

    name = '-'.join([
        str(int(time.time())),
        args.zone,
        args.dist,
    ])

    if args.append_cluster_name is not None:
        suffix = args.append_cluster_name
        if not suffix.startswith('-'):
            suffix = '-' + suffix
        identities = isilon_hadoop_tools.identities.with_suffix_applied(identities, suffix)
        name += suffix

    onefs_and_files = isilon_hadoop_tools.identities.Creator(
        onefs=onefs,
        onefs_zone=args.zone,
        start_uid=args.start_uid,
        start_gid=args.start_gid,
        script_path=os.path.join(os.getcwd(), name + '.sh'),
    )
    if args.dry:
        LOGGER.info(DRY_RUN)
        LOGGER.info('A script would have been created at %s.', onefs_and_files.script_path)
        LOGGER.info('The following actions would have populated it and OneFS:')
        onefs_and_files.log_identities(identities)
    else:
        onefs_and_files.create_identities(identities)


def isilon_create_directories_cli(parser=None):
    """Define CLI arguments and options for isilon_create_directories."""
    if parser is None:
        parser = base_cli()
    return parser


@isilon_hadoop_tools.cli.catches(isilon_hadoop_tools.IsilonHadoopToolError)
def isilon_create_directories(argv=None):
    """Execute isilon_create_directories commands."""

    if argv is None:
        argv = sys.argv[1:]
    args = isilon_create_directories_cli().parse_args(argv)

    isilon_hadoop_tools.cli.configure_logging(args)
    configure_script(args)
    onefs = isilon_hadoop_tools.cli.hdfs_client(args)

    suffix = args.append_cluster_name
    if suffix is not None and not suffix.startswith('-'):
        suffix = '-' + suffix

    directories = {
        'cdh': isilon_hadoop_tools.directories.cdh_directories,
        'hdp': isilon_hadoop_tools.directories.hdp_directories,
    }[args.dist](identity_suffix=suffix)

    creator = isilon_hadoop_tools.directories.Creator(
        onefs=onefs,
        onefs_zone=args.zone,
    )
    try:
        if args.dry:
            LOGGER.info(DRY_RUN)
            creator.log_directories(directories)
        else:
            creator.create_directories(directories)
    except isilon_hadoop_tools.directories.HDFSRootDirectoryError as exc:
        raise_from(
            isilon_hadoop_tools.cli.CLIError(
                'The HDFS root directory must not be {0}.'.format(exc),
            ),
            exc,
        )
