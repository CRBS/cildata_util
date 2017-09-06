#! /usr/bin/env python


import argparse
import sys
import logging
import os


import cildata_util
from cildata_util import dbutil
from cildata_util import config
from cildata_util.dbutil import CILDataFileListFromJsonPickleFactory
from cildata_util.dbutil import CILDataFileJsonPickleWriter

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
    reader = CILDataFileListFromJsonPickleFactory()
    cdf_list = reader.get_cildatafiles(theargs.jsonfile)
    abs_json_path = os.path.abspath(theargs.jsonfile)
    json_dir = os.path.dirname(abs_json_path)
    updated = False
    for cdf in cdf_list:
        if cdf.get_file_size() is None:
            local_file_fp = os.path.join(json_dir,
                                         cdf.get_file_name())
            if not os.path.isfile(local_file_fp):
                logger.warning(local_file_fp + ' file not found. '
                                               'Skipping update')
                continue
            logger.debug('Updating size for file: ' + local_file_fp)
            cdf.set_file_size(os.path.getsize(local_file_fp))
            updated = True

    if updated is True:
        logger.debug('Updating json file ' + theargs.jsonfile)
        dbutil.make_backup_of_json(theargs.jsonfile)

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
