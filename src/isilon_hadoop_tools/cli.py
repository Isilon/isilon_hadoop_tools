"""This module defines a CLI common to all command-line tools."""

from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import getpass
import logging

from future.utils import raise_from

import isilon_hadoop_tools.onefs


__all__ = [
    # Decorators
    'catches',

    # Exceptions
    'CLIError',
    'HintedError',

    # Functions
    'base_cli',
    'configure_logging',
    'hdfs_client',
    'logging_cli',
    'onefs_cli',
    'onefs_client',
]

LOGGER = logging.getLogger(__name__)


class CLIError(isilon_hadoop_tools.IsilonHadoopToolError):
    """All Exceptions emitted from this module inherit from this Exception."""


def catches(exception):
    """Create a decorator for functions that emit the specified exception."""
    def decorator(func):
        """Decorate a function that should catch instances of the specified exception."""
        def decorated(*args, **kwargs):
            """Catch instances of a specified exception that are raised from the function."""
            try:
                return func(*args, **kwargs)
            except exception as ex:
                logging.error(ex)
                return 1
        return decorated
    return decorator


def base_cli(parser=None):
    """Define common CLI arguments and options."""
    if parser is None:
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    onefs_cli(parser.add_argument_group('OneFS'))
    logging_cli(parser.add_argument_group('Logging'))
    return parser


def onefs_cli(parser=None):
    """Define OneFS CLI arguments and options."""
    if parser is None:
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--zone", "-z",
        help="Specify a OneFS access zone.",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--no-verify",
        help="Do not verify SSL/TLS certificates.",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--onefs-password",
        help="Specify the password for --onefs-user.",
        type=str,
    )
    parser.add_argument(
        "--onefs-user",
        help="Specify the user to connect to OneFS as.",
        type=str,
        default="root",
    )
    parser.add_argument(
        "onefs_address",
        help="Specify an IP address or FQDN/SmartConnect that "
        "can be used to connect to and configure OneFS.",
        type=str,
    )
    return parser


class HintedError(CLIError):

    """
    This exception is used to modify the error message passed to the user
    when a common error occurs that has a possible solution the user will likely want.
    """

    def __str__(self):
        base_str = super(HintedError, self).__str__()
        return str(getattr(self, '__cause__', None)) + '\nHint: ' + base_str


def _client_from_onefs_cli(init, args):
    try:
        return init(
            address=args.onefs_address,
            username=args.onefs_user,
            password=getpass.getpass() if args.onefs_password is None else args.onefs_password,
            default_zone=args.zone,
            verify_ssl=not args.no_verify,
        )
    except isilon_hadoop_tools.onefs.OneFSCertificateError as exc:
        raise_from(
            HintedError('--no-verify can be used to skip certificate verification.'),
            exc,
        )
    except isilon_hadoop_tools.onefs.MissingLicenseError as exc:
        raise_from(
            CLIError(
                (
                    isilon_hadoop_tools.onefs.APIError.license_expired_error_format
                    if isinstance(exc, isilon_hadoop_tools.onefs.ExpiredLicenseError) else
                    isilon_hadoop_tools.onefs.APIError.license_missing_error_format
                ).format(exc),
            ),
            exc,
        )
    except isilon_hadoop_tools.onefs.MissingZoneError as exc:
        raise_from(
            CLIError(isilon_hadoop_tools.onefs.APIError.zone_not_found_error_format.format(exc)),
            exc,
        )


def hdfs_client(args):
    """Get a onefs.Client.for_hdfs from args parsed by onefs_cli."""
    return _client_from_onefs_cli(isilon_hadoop_tools.onefs.Client.for_hdfs, args)


def onefs_client(args):
    """Get a onefs.Client from args parsed by onefs_cli."""
    return _client_from_onefs_cli(isilon_hadoop_tools.onefs.Client, args)


def logging_cli(parser=None):
    """Define logging CLI arguments and options."""
    if parser is None:
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-q', '--quiet',
        default=False,
        action='store_true',
        help='Supress console output.',
    )
    parser.add_argument(
        "--log-file",
        type=str,
        help="Specify a path to log to.",
    )
    parser.add_argument(
        "--log-level",
        help="Specify how verbose logging should be.",
        default='info',
        choices=('debug', 'info', 'warning', 'error', 'critical'),
    )
    return parser


def configure_logging(args):
    """Configure logging for command-line tools."""
    logging.getLogger().setLevel(logging.getLevelName(args.log_level.upper()))
    if not args.quiet:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))
        logging.getLogger().addHandler(console_handler)
    if args.log_file:
        logfile_handler = logging.FileHandler(args.log_file)
        logfile_handler.setFormatter(
            logging.Formatter('[%(asctime)s] %(name)s [%(levelname)s] %(message)s'),
        )
        logging.getLogger().addHandler(logfile_handler)
