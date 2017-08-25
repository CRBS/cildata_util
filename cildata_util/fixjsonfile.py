#! /usr/bin/env python


import argparse
import sys
import logging
import os
import requests
import shutil
import time
import hashlib

import cildata_util
from cildata_util import config
from cildata_util.dbutil import CILDataFileFromDatabaseFactory
from cildata_util.dbutil import CILDataFileListFromJsonPickleFactory
from cildata_util.dbutil import CILDataFileJsonPickleWriter
from cildata_util.dbutil import CILDataFileFoundInFilesystemFilter

logger = logging.getLogger('cildata_util.fixjsonfile')


def _parse_arguments(desc, args):
    """Parses command line arguments
    """
    help_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=help_formatter)

    parser.add_argument("jsonfile", help='json file to fix')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')

    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + cildata_util.__version__))
    return parser.parse_args(args)


def _fix_json_file(theargs):
    if os.path.isfile(theargs.jsonfile + '.orig'):
        logger.warning(theargs.jsonfile + '.orig exists already. skipping...')
        return

    reader = CILDataFileListFromJsonPickleFactory(fixheaders=True)
    cdf_list = reader.get_cildatafiles(theargs.jsonfile)
    shutil.copy(theargs.jsonfile, theargs.jsonfile + '.orig')
    writer = CILDataFileJsonPickleWriter()
    writer.writeCILDataFileListToFile(theargs.jsonfile,
                                      cdf_list,
                                      skipsuffixappend=True)
    return 0

def main(args):
    """Main entry into fixjsonfile
    :param args: should be set to sys.argv aka the list of arguments
                 starting with script name as first argument
    :returns: exit code of 0 upon success otherwise failure
    """

    desc = """
              Version {version}

              This program fixes the json file passed in by
              replacing the headers data with a basic dictionary
              instead of the requests object which does not
              properly load in jsonpickle
    """.format(version=cildata_util.__version__)

    theargs = _parse_arguments(desc, args[1:])
    theargs.program = args[0]
    theargs.version = cildata_util.__version__
    config.setup_logging(logger, loglevel=theargs.loglevel)

    try:
        return _fix_json_file(theargs)
    except Exception as e:
        logger.exception('Caught fatal exception')
        return 1


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
