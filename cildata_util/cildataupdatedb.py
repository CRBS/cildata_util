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
from cildata_util.dbutil import CILDataFileFromJsonFilesFactory
from cildata_util.dbutil import CILDataFileListFromJsonPickleFactory
from cildata_util.dbutil import CILDataFileNoRawFilter
from cildata_util.dbutil import CILDataFileDatabaseUpdater
logger = logging.getLogger('cildata_util.cildataupdatedb')


def _parse_arguments(desc, args):
    """Parses command line arguments
    """
    help_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=help_formatter)
    parser.add_argument("databaseconf", help='Database configuration file')
    parser.add_argument("downloaddir",
                        help='Directory where images and videos reside')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument('--id', help='Only update database on '
                                     'data with id passed in.')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + cildata_util.__version__))
    return parser.parse_args(args)


def _update_database(theargs):
    """Examine all downloaded data and retry any
       failed entries
    """
    abs_destdir = os.path.abspath(theargs.downloaddir)
    images_destdir = os.path.join(abs_destdir, dbutil.IMAGES_DIR)
    videos_destdir = os.path.join(abs_destdir, dbutil.VIDEOS_DIR)
    fac = CILDataFileFromJsonFilesFactory()
    all_cdf = fac.get_cildatafiles(abs_destdir)

    logger.info('Total entries: ' + str(len(all_cdf)))

    nofailedrawfilt = CILDataFileNoRawFilter()
    filt_cdf = nofailedrawfilt.get_cildatafiles(all_cdf)

    logger.debug('Reading database config')
    dbconf = CILDatabaseConfig(theargs.databaseconf)
    db = Database(dbconf)
    conn = db.get_connection()
    updater = CILDataFileDatabaseUpdater(conn)

    try:
        reader = CILDataFileListFromJsonPickleFactory()

        for cdf in filt_cdf:
            if theargs.id is not None:
                if theargs.id != str(cdf.get_id()):
                    continue
            logger.debug(cdf.get_file_name())
            if cdf.get_is_video():
                base_dir = os.path.join(videos_destdir, str(cdf.get_id()))
            else:
                base_dir = os.path.join(images_destdir, str(cdf.get_id()))
                jsonfile = os.path.join(base_dir, str(cdf.get_id()) + dbutil.JSON_SUFFIX)
                if not os.path.isfile(jsonfile):
                    logger.error(str(cdf.get_file_name()) + ' was guessed to be image, '
                                                            'but this is wrong'
                                                            'going with video')
                    base_dir = os.path.join(videos_destdir, str(cdf.get_id()))

            # update database
            logger.debug('Update database')
            updater.insert_cildatafiles([cdf])
    finally:
        if conn is not None:
            conn.close()
    return 0


def main(args):
    """Main entry into cildataupdatedb
    :param args: should be set to sys.argv aka the list of arguments
                 starting with script name as first argument
    :returns: exit code of 0 upon success otherwise failure
    """

    desc = """
              Version {version}

              Given a directory of images and videos (1st argument)
              converted by cildataconverter.py, this script adds the datasets
              to the database specified by the configuration file
              passed in as the first argument to this script.

              This script uses the #.json file that is stored within each
              dataset folder for most of the information.

              The database table is expected to have this structure
              as shown from output of psql \d:

                          Table "public.cil_download_status"
                    Column      |            Type             | Modifiers
              ------------------+-----------------------------+-----------
               id               | bigint                      | not null
               image_id         | bigint                      |
               is_video         | boolean                     |
               file_name        | text                        |
               download_success | boolean                     |
               download_time    | timestamp without time zone |
               checksum         | boolean                     |
               mime_type        | text                        |
               num_of_bytes     | bigint                      |
               checksum_value   | text                        |
               Indexes:
                   "download_status_pk" PRIMARY KEY, btree (id)


              For more information please visit:

              https://github.com/slash-segmentation/CIL_file_download_tool/wiki
    """.format(version=cildata_util.__version__)

    theargs = _parse_arguments(desc, args[1:])
    theargs.program = args[0]
    theargs.version = cildata_util.__version__
    config.setup_logging(logger, loglevel=theargs.loglevel)

    try:
        return _update_database(theargs)
    except Exception:
        logger.exception('Caught fatal exception')
        return 1


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
