#! /usr/bin/env python


import argparse
import sys
import logging
import os
import cildata_util
from cildata_util import config
from cildata_util import dbutil
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


def _get_cildatafiles_from_database(theargs):
    """Queries database for a list of CILDataFile objects
    """
    logger.debug('Reading database config')
    dbconf = CILDatabaseConfig(theargs.databaseconf)
    db = Database(dbconf)
    logger.debug('Getting database connection')
    conn = None
    try:
        conn = db.get_connection()
        fac = CILDataFileFromDatabaseFactory(conn)
        cildatafiles = fac.get_cildatafiles()
        logger.info('Found ' + str(len(cildatafiles)) + ' entries')
        return cildatafiles
    finally:
        if conn is not None:
            conn.close()
    return None


def _get_hashof_cildatafiles_by_file_name(cildatafiles):
    """Creates a dictionary from list of CILDataFile objects
       passed in by setting key to
       CILDataFile.get_file_name() and value
       set to the CILDataFile object. NOTE: Only entries
       that end with .raw are put into the hash. The other
       entries are ignored.
       :param cildatafiles: list of CILDataFile objects
       :returns: dictionary of CILDataFile objects
    """
    cildatafile_hash = {}
    for entry in cildatafiles:
            cildatafile_hash[entry.get_file_name()] = entry
    return cildatafile_hash


def _update_cil_data_files_for_id(cdf_list, cildatafile_hash):
    """Using hash updates a CILDataFile objects in cdf_list
    :param cdf_list: list of CILDataFile objects that belong to
                     same id
    :param cildatafile_hash: dictionary [get_file_name()] => CILDataFile
    :returns: tuple (cdf_list, boolean to denote if updated)
    """
    update_needed = False
    for cdf in cdf_list:
        if cdf.get_file_name() not in cildatafile_hash:
            continue
        db_cdf = cildatafile_hash[cdf.get_file_name()]

        if cdf.get_has_raw() != db_cdf.get_has_raw():
            update_needed = True
            cdf.set_has_raw(db_cdf.get_has_raw())
    return cdf_list, update_needed


def _update_cil_data_files(theargs):
    cildatafiles = _get_cildatafiles_from_database(theargs)
    if cildatafiles is None:
        logger.error('No CILDataFiles obtained from database')
        return 1
    cildatafile_hash = _get_hashof_cildatafiles_by_file_name(cildatafiles)

    if len(cildatafile_hash) is 0:
        logger.error('Hash of CILDataFile objects is empty')
        return 2

    return _write_updated_cil_data_files(theargs, cildatafile_hash)


def _write_updated_cil_data_files(theargs, cildatafile_hash):
    """Does the download"""
    factory = CILDataFileFromJsonFilesFactory()
    abs_destdir = os.path.abspath(theargs.downloaddir)
    images_destdir = os.path.join(abs_destdir, 'images')
    writer = CILDataFileJsonPickleWriter()

    for entry in os.listdir(images_destdir):
        fp = os.path.join(images_destdir, entry)
        if not os.path.isdir(fp):
            continue

        cdf_list = factory.get_cildatafiles(fp)
        if theargs.id is not None:
            if theargs.id != str(cdf_list[0].get_id()):
                continue

        cdf_list, rewrite = _update_cil_data_files_for_id(cdf_list,
                                                          cildatafile_hash)
        if rewrite is True:
            logger.info('Rewriting cdf list file in this directory ' + fp)
            theid = os.path.basename(fp)
            json_file = os.path.join(fp, theid + dbutil.JSON_SUFFIX)
            dbutil.make_backup_of_json(json_file)
            writer.writeCILDataFileListToFile(json_file, cdf_list,
                                              skipsuffixappend=True)
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
    except Exception:
        logger.exception('Caught fatal exception')
        return 1


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
