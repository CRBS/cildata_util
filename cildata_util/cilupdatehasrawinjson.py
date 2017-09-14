#! /usr/bin/env python


import argparse
import sys
import logging
import os
import cildata_util
from cildata_util import config
from cildata_util.config import CILDatabaseConfig
from cildata_util.dbutil import Database
from cildata_util.dbutil import CILDataFileFromDatabaseFactory
from cildata_util.dbutil import CILDataFileJsonPickleWriter
from cildata_util.dbutil import CILDataFileFromJsonFilesFactory
from cildata_util.dbutil import CILDataFileListFromJsonPickleFactory


logger = logging.getLogger('cildata_util.cilupdatehasrawinjson')


def _parse_arguments(desc, args):
    """Parses command line arguments
    """
    help_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=help_formatter)

    parser.add_argument("databaseconf", help='Database configuration file')
    parser.add_argument("downloaddir",
                        help='Directory where images and videos are saved')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument("--id", help='Only update entry with this id')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + cildata_util.__version__))
    return parser.parse_args(args)


def _update_cil_data_files(theargs):
    """Does the download"""
    download_dir = os.path.abspath(theargs.downloaddir)
    factory = CILDataFileFromJsonFilesFactory()
    logger.debug('Reading database config')
    dbconf = CILDatabaseConfig(theargs.databaseconf)
    db = Database(dbconf)
    logger.debug('Getting database connection')
    abs_destdir = os.path.abspath(theargs.downloaddir)
    images_destdir = os.path.join(abs_destdir, 'images')
    conn = None
    reader = CILDataFileListFromJsonPickleFactory()
    writer = CILDataFileJsonPickleWriter()
    try:

        conn = db.get_connection()
        fac = CILDataFileFromDatabaseFactory(conn)
        cildatafiles = fac.get_cildatafiles()
        logger.info('Found ' + str(len(cildatafiles)) + ' entries')

        # build hash table
        cildatafile_hash = {}
        for entry in cildatafiles:
            if entry.get_file_name().endswith('.raw'):
                cildatafile_hash[entry.get_file_name()] = entry

        no_raw_counter = 0
        for entry in os.listdir(images_destdir):
            fp = os.path.join(images_destdir, entry)
            if not os.path.isdir(fp):
                continue

            rewrite_cdflist = False
            cdf_list = factory.get_cildatafiles(fp)

            for cdf in cdf_list:
                if theargs.id is not None:
                    if theargs.id == str(cdf.get_id()):
                        logger.info('Id matches!!!!')
                        rewrite_cdflist = False

                        break

                if cdf.get_file_name() not in cildatafile_hash:
                    continue
                db_cdf = cildatafile_hash[cdf.get_file_name()]

                if db_cdf.get_has_raw() is None:
                    logger.error(cdf.get_file_name() + ' has raw is None')
                    break

                if db_cdf.get_has_raw() == True:
                    break

                cdf.set_has_raw(db_cdf.get_has_raw())
                # logger.debug(cdf.get_file_name() + ' has raw is ' +
                #             str(cdf.get_has_raw()))
                no_raw_counter += 1
                rewrite_cdflist = True
                break

            if rewrite_cdflist is True:
                logger.info('Rewriting cdf list file in this directory' + fp)

        logger.info('Found ' + str(no_raw_counter) + ' raw entries that'
                                                     'are NOT supposed'
                                                     ' to have raw files')
    finally:
        if conn is not None:
            conn.close()

    return 0


def main(args):
    """Main entry into cilfiledownloader
    :param args: should be set to sys.argv aka the list of arguments
                 starting with script name as first argument
    :returns: exit code of 0 upon success otherwise failure
    """

    desc = """
              Version {version}
    """.format(version=cildata_util.__version__)

    theargs = _parse_arguments(desc, args[1:])
    theargs.program = args[0]
    theargs.version = cildata_util.__version__
    config.setup_logging(logger, loglevel=theargs.loglevel)

    try:
        return _update_cil_data_files(theargs)
    except Exception as e:
        logger.exception('Caught fatal exception')
        return 1


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
