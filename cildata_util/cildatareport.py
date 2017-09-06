#! /usr/bin/env python


import argparse
import sys
import logging
import os
import shutil
import re
import cildata_util
from cildata_util import dbutil
from cildata_util import config
from cildata_util.dbutil import CILDataFile
from cildata_util.dbutil import CILDataFileListFromJsonPickleFactory
from cildata_util.dbutil import CILDataFileJsonPickleWriter
from cildata_util.dbutil import CILDataFileFromJsonFilesFactory

logger = logging.getLogger('cildata_util.cildatareport')


def _parse_arguments(desc, args):
    """Parses command line arguments
    """
    help_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=help_formatter)

    parser.add_argument("downloaddir", help='Download directory')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + cildata_util.__version__))
    return parser.parse_args(args)


def _generate_report(theargs):
    download_dir = os.path.abspath(theargs.downloaddir)
    factory = CILDataFileFromJsonFilesFactory()
    counter = 0
    mimetypes = {}
    failed_count = 0
    failed_list = []
    failed_id_hash = {}
    for cdf in factory.get_cildatafiles(download_dir):
        if cdf.get_id() not in failed_id_hash:
            failed_id_hash[cdf.get_id()] = False

        if cdf.get_mime_type() not in mimetypes:
            mimetypes[cdf.get_mime_type()] = 1
        else:
            mimetypes[cdf.get_mime_type()] += 1
        counter += 1
        if cdf.get_download_success() is False:
            failed_id_hash[cdf.get_id()] = True
            failed_count += 1
            failed_list.append(cdf)
            if cdf.get_mime_type() is not None and cdf.get_mime_type().startswith('text'):
                logger.debug('Failed and type is text: ' + cdf.get_file_name())
        else:
            if cdf.get_mime_type() is None:
                logger.debug(cdf.get_file_name() + 'mime type is None')
            else:
                if cdf.get_mime_type().startswith('text'):
                    logger.debug('Success, but type is text: ' + cdf.get_file_name())

    for entry in failed_list:
        sys.stdout.write(entry.get_file_name() + '\n')

    sys.stdout.write('# entries: ' + str(counter) + '\n')
    sys.stdout.write('# failed: ' + str(failed_count) + '\n')

    num_ids = len(failed_id_hash.keys())
    failedcnt = 0
    for entry in failed_id_hash.keys():
        if failed_id_hash[entry] is True:
            failedcnt += 1
    sys.stdout.write('# ids ' + str(num_ids) + ' and # failed: ' + str(failedcnt) + '\n')

    sys.stdout.write('-----------------\n')
    for entry in mimetypes.keys():
        sys.stdout.write('\t' + str(entry) + ' ==> ' + str(mimetypes[entry]) + '\n')
    return 0


def main(args):
    """Main entry into fixjsonfile
    :param args: should be set to sys.argv aka the list of arguments
                 starting with script name as first argument
    :returns: exit code of 0 upon success otherwise failure
    """

    desc = """
              Version {version}

              This program examines a directory passed in and
              using all the .json files found generates a
              report about the downloaded data.
    """.format(version=cildata_util.__version__)

    theargs = _parse_arguments(desc, args[1:])
    theargs.program = args[0]
    theargs.version = cildata_util.__version__
    config.setup_logging(logger, loglevel=theargs.loglevel)

    try:
        return _generate_report(theargs)
    except Exception as e:
        logger.exception('Caught fatal exception')
        return 1


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
