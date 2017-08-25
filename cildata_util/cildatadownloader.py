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
from cildata_util.dbutil import CILDataFileFoundInFilesystemFilter

logger = logging.getLogger('cildata_util.cildatadownloader')


def _parse_arguments(desc, args):
    """Parses command line arguments
    """
    help_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=help_formatter)

    parser.add_argument("databaseconf", help='Database configuration file')
    parser.add_argument("destdir",
                        help='Directory where images and videos will be saved')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument('--id', help='Only download data with id passed in.')
    parser.add_argument('--skipifexists', action='store_true',
                        help='Skip download if directory for id exists '
                             'on filesystem')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + cildata_util.__version__))
    return parser.parse_args(args)


def _download_cil_data_files(theargs):
    """Does the download"""
    logger.debug('Reading database config')
    dbconf = CILDatabaseConfig(theargs.databaseconf)
    db = Database(dbconf)
    logger.debug('Getting database connection')
    abs_destdir = os.path.abspath(theargs.destdir)
    images_destdir = os.path.join(abs_destdir, 'images')
    videos_destdir = os.path.join(abs_destdir, 'videos')
    conn = None
    last_id = -1
    last_outdir = None
    same_id_cdf_list = []
    writer = CILDataFileJsonPickleWriter()
    try:

        conn = db.get_connection()
        fac = CILDataFileFromDatabaseFactory(conn, id=theargs.id)
        cildatafiles = fac.get_cildatafiles()
        logger.info('Found ' + str(len(cildatafiles)) + ' entries')

        if theargs.skipifexists:
            logger.info("--skipifexists set to true. Skipping download if"
                        " id exists on filesystem")
            fs_cdf_filter = CILDataFileFoundInFilesystemFilter(images_destdir,
                                                               videos_destdir)
            filt_list = fs_cdf_filter.get_cildatafiles(cildatafiles)
            logger.info('After filtering found ' + str(len(filt_list)) +
                        ' entries')
            cildatafiles = filt_list

        for entry in cildatafiles:

            logger.info('Downloading ' + str(entry.get_id()))
            if entry.get_is_video():
                out_dir = videos_destdir
            else:
                out_dir = images_destdir
            if last_id == entry.get_id():
                loadbaseurl = False
            else:
                loadbaseurl = True
                if len(same_id_cdf_list) > 0:

                    writerfile = os.path.join(last_outdir, str(last_id),
                                              str(last_id))
                    writer.writeCILDataFileListToFile(writerfile,
                                                      same_id_cdf_list)
                    same_id_cdf_list = []

            last_id = entry.get_id()
            last_outdir = out_dir
            cdf = dbutil.download_cil_data_file(out_dir, entry,
                                                loadbaseurl=loadbaseurl)
            same_id_cdf_list.append(cdf)

        if len(same_id_cdf_list) > 0:
            writerfile = os.path.join(last_outdir, str(last_id),
                                      str(last_id))
            writer.writeCILDataFileListToFile(writerfile,
                                              same_id_cdf_list)
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
        return _download_cil_data_files(theargs)
    except Exception as e:
        logger.exception('Caught fatal exception')
        return 1


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
