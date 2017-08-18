#! /usr/bin/env python


import argparse
import sys
import logging
import os
import requests
import shutil

import cildata_util
from cildata_util import config
from cildata_util.config import CILDatabaseConfig
from cildata_util.dbutil import Database
from cildata_util.dbutil import CILDataFileFromDatabaseFactory

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
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + cildata_util.__version__))
    return parser.parse_args(args)


def _download_file(url, dest_dir):

    logger.debug('Downloading from ' + url)
    local_filename = url.split('/')[-1]
    dest_file = os.path.join(dest_dir, local_filename)
    r = requests.get(url, stream=True)

    with open(dest_file, 'wb') as f:
        shutil.copyfileobj(r.raw, f)
    logger.debug(r.headers)
    return local_filename, r.headers['Content-Type'], r.status_code


def _get_download_url(base_url, omero_url, cdf):

    if cdf.get_file_name().endswith('.flv'):
        return base_url + 'videos/'

    if cdf.get_file_name().endswith('.jpg'):
        return base_url + 'images/download_jpeg/'

    if cdf.get_file_name().endswith('.tif'):
        return omero_url

    if cdf.get_file_name().endswith('.raw'):
        return omero_url
    logger.error('Not sure how to download this file: ' +
                 cdf.get_file_name())
    return None


def _download_cil_data_file(destination_dir, cdf, loadbaseurl=False):

    base_url = 'http://www.cellimagelibrary.org/'
    omero_url = 'http://grackle.crbs.ucsd.edu:8080/OmeroWebService/images/'
    str_id = str(cdf.get_id())

    out_dir = os.path.join(destination_dir, str_id)
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir, mode=0o755)
    if loadbaseurl is True:
        # hit download page
        logger.debug('Loading base url')
        aurl = base_url + 'images/' + str_id
        r = requests.get(aurl)
        if r.status_code is not 200:
            logger.warning('Hitting ' + aurl + ' returned status of ' +
                           str(r.status_code))

    logger.info('Downloading file: ' + cdf.get_file_name())
    download_url = _get_download_url(base_url, omero_url, cdf)
    if download_url is None:
        return
    local_file, content_type, status = _download_file(download_url +
                                                      cdf.get_file_name(),
                                                      out_dir)
    logger.debug('content type: ' + content_type)
    logger.debug('status: ' + str(status))
    cdf.set_mime_type(content_type)
    cdf.set_localfile(local_file)
    if status is 200:
        cdf.set_download_success(True)
    else:
        cdf.set_download_success(False)

    return cdf


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
    try:

        conn = db.get_connection()
        fac = CILDataFileFromDatabaseFactory(conn)
        cildatafiles = fac.get_cildatafiles()
        logger.info('Found ' + str(len(cildatafiles)) + ' entries')

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
            last_id = entry.get_id()
            cdf = _download_cil_data_file(out_dir, entry, loadbaseurl=loadbaseurl)
        
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

    return _download_cil_data_files(theargs)


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
