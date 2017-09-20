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
from cildata_util.dbutil import CILDataFileNoRawFilter
from cildata_util.dbutil import CILDataFileFromJsonFilesFactory
from cildata_util.dbutil import CILDataFileFailedDownloadFilter
from cildata_util.dbutil import CILDataFileListFromJsonPickleFactory

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
    parser.add_argument('--retryfailed', action='store_true',
                        help='Going off of filesystem retry any failed'
                             'downloads')
    parser.add_argument('--numretries', type=int, default=2,
                        help='Number of attempts to make at downloading'
                             ' a file')
    parser.add_argument('--retrysleep', type=int, default=30,
                        help='Number of seconds to wait before'
                             ' retrying a download')
    parser.add_argument('--timeout', type=int, default=120,
                        help='Number of seconds to wait for response'
                             ' from http when downloading a file')

    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + cildata_util.__version__))
    return parser.parse_args(args)


def _retry_download_of_failed(theargs):
    """Examine all downloaded data and retry any
       failed entries
    """
    abs_destdir = os.path.abspath(theargs.destdir)
    images_destdir = os.path.join(abs_destdir, dbutil.IMAGES_DIR)
    videos_destdir = os.path.join(abs_destdir, dbutil.VIDEOS_DIR)
    fac = CILDataFileFromJsonFilesFactory()
    all_cdf = fac.get_cildatafiles(abs_destdir)

    logger.info('Total entries: ' + str(len(all_cdf)))

    filter = CILDataFileFailedDownloadFilter()
    failfilt_cdf = filter.get_cildatafiles(all_cdf)
    no_rawfilt = CILDataFileNoRawFilter()
    filt_cdf = no_rawfilt.get_cildatafiles(failfilt_cdf)
    logger.info('Failed entries: ' + str(len(filt_cdf)))

    reader = CILDataFileListFromJsonPickleFactory()
    writer = CILDataFileJsonPickleWriter()

    for cdf in filt_cdf:
        if theargs.id is not None:
            if theargs.id != str(cdf.get_id()):
                continue

        if cdf.get_is_video():
            base_dir = os.path.join(videos_destdir, str(cdf.get_id()))
        else:
            base_dir = os.path.join(images_destdir, str(cdf.get_id()))

        destfile = os.path.join(base_dir, cdf.get_file_name())
        if os.path.isfile(destfile):
            logger.info(destfile + ' exists. Removing...')
            os.remove(destfile)
        newcdf = dbutil.download_cil_data_file(base_dir, cdf,
                                               loadbaseurl=True,
                                               download_direct_to_dest=True,
                                               numretries=theargs.numretries,
                                               retry_sleep=theargs.retrysleep,
                                               timeout=theargs.timeout)
        jsonfile = os.path.join(base_dir, str(cdf.get_id()) + dbutil.JSON_SUFFIX)

        logger.debug('Making backup of ' + jsonfile)
        dbutil.make_backup_of_json(jsonfile)

        cdf_list = reader.get_cildatafiles(jsonfile)

        newcdf_list = []
        for entry in cdf_list:
            if entry.get_file_name() == newcdf.get_file_name():
                logger.info('Updating ' + newcdf.get_file_name())
                newcdf_list.append(newcdf)
                continue
            newcdf_list.append(entry)

        writer.writeCILDataFileListToFile(jsonfile, newcdf_list,
                                          skipsuffixappend=True)

    return 0


def _download_cil_data_files(theargs):
    """Does the download"""
    logger.debug('Reading database config')
    dbconf = CILDatabaseConfig(theargs.databaseconf)
    db = Database(dbconf)
    logger.debug('Getting database connection')
    abs_destdir = os.path.abspath(theargs.destdir)
    images_destdir = os.path.join(abs_destdir, dbutil.IMAGES_DIR)
    videos_destdir = os.path.join(abs_destdir, dbutil.VIDEOS_DIR)
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

        noraw_filt = CILDataFileNoRawFilter()
        cildatafiles = noraw_filt.get_cildatafiles(cildatafiles)
        logger.info('Skipped raw without download count: ' +
                    str(len(cildatafiles)))

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
                                                loadbaseurl=loadbaseurl,
                                                numretries=theargs.numretries,
                                                retry_sleep=theargs.retrysleep,
                                                timeout=theargs.timeout)
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
        if theargs.retryfailed is True:
            return _retry_download_of_failed(theargs)

        return _download_cil_data_files(theargs)
    except Exception:
        logger.exception('Caught fatal exception')
        return 1


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
