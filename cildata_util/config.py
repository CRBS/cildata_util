__author__ = 'churas'

import configparser
import os
import logging

logger = logging.getLogger(__name__)


class CILDatabaseConfigMissingError(Exception):
    """Raised if configuration file is missing
    """
    pass


class CILDatabaseConfigParseError(Exception):
    """Raised if unable to extract a value from config
    """
    pass


def setup_logging(thelogger,
                  log_format='%(asctime)-15s %(levelname)s %(name)s '
                             '%(message)s',
                  loglevel='WARNING'):
    """Sets up logging
    """
    if loglevel == 'DEBUG':
        numericloglevel = logging.DEBUG
    if loglevel == 'INFO':
        numericloglevel = logging.INFO
    if loglevel == 'WARNING':
        numericloglevel = logging.WARNING
    if loglevel == 'ERROR':
        numericloglevel = logging.ERROR
    if loglevel == 'CRITICAL':
        numericloglevel = logging.CRITICAL

    logger.setLevel(numericloglevel)
    thelogger.setLevel(numericloglevel)
    logging.basicConfig(format=log_format)
    logging.getLogger('cildata_util.config').setLevel(numericloglevel)
    logging.getLogger('cildata_util.dbutil').setLevel(numericloglevel)


class CILDatabaseConfig(object):
    """Objects of this class parse CIL database configuration
    """
    POSTGRES_SECTION = 'postgres'
    POSTGRES_USER = 'user'
    POSTGRES_PASS = 'password'
    POSTGRES_HOST = 'host'
    POSTGRES_PORT = 'port'
    POSTGRES_DB = 'database'

    def __init__(self, config_file):
        """Constructor
        :param config_file: Path to config file to parse
        """
        if config_file is None:
            raise CILDatabaseConfigMissingError('None passed in for '
                                                'config file')

        if not os.path.isfile(config_file):
            raise CILDatabaseConfigMissingError('Config file not found on '
                                                'filesystem or is not a file')
        self._config = configparser.ConfigParser()
        self._config.read(config_file)

    def _get_param(self, param):

        if not self._config.has_section(CILDatabaseConfig.POSTGRES_SECTION):
            raise CILDatabaseConfigParseError('No postgres section '
                                              'found in config')

        if not self._config.has_option(CILDatabaseConfig.POSTGRES_SECTION,
                                       param):
            raise CILDatabaseConfigParseError(param +
                                              ' not found in configuration')

        return self._config.get(CILDatabaseConfig.POSTGRES_SECTION, param)

    def get_user(self):
        return self._get_param(CILDatabaseConfig.POSTGRES_USER)

    def get_password(self):
        return self._get_param(CILDatabaseConfig.POSTGRES_PASS)

    def get_host(self):
        return self._get_param(CILDatabaseConfig.POSTGRES_HOST)

    def get_port(self):
        return int(self._get_param(CILDatabaseConfig.POSTGRES_PORT))

    def get_database_name(self):
        return self._get_param(CILDatabaseConfig.POSTGRES_DB)
