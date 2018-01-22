#! /usr/bin/env python


import argparse
import sys
import logging
import os
import cildata_util
from cildata_util import config
from cildata_util.dbutil import CILDataFileFromJsonFilesFactory
from cildata_util.dbutil import CILDataFileNoRawFilter

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
    parser.add_argument("--printfailed", action='store_true',
                        help='If set output file names of files that'
                             'failed to download')
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
    unique_ids = {}
    not_supposed_to_have_raw = 0
    noraw_filt = CILDataFileNoRawFilter()
    cdf_list = factory.get_cildatafiles(download_dir)
    logger.info('Unfiltered list count: ' + str(len(cdf_list)))

    filt_cdf_list = noraw_filt.get_cildatafiles(cdf_list)
    logger.info('Skipped raw without download count: ' +
                str(len(filt_cdf_list)))
    not_supposed_to_have_raw = len(cdf_list) - len(filt_cdf_list)

    if theargs.printfailed is True:
        sys.stdout.write('Failed\n')
    for cdf in filt_cdf_list:
        unique_ids[cdf.get_id()] = 1
        counter += 1
        if cdf.get_download_success() is False:
            failed_id_hash[cdf.get_id()] = True
            if theargs.printfailed is True:
                sys.stdout.write(cdf.get_file_name() + '\n')
            failed_count += 1
            failed_list.append(cdf)
        else:
            if cdf.get_file_size() is 0:
                failed_id_hash[cdf.get_id()] = True
                if theargs.printfailed is True:
                    sys.stdout.write(cdf.get_file_name() + '\n')
                failed_count += 1
                failed_list.append(cdf)
            else:
                if cdf.get_mime_type() is None:
                    logger.debug(cdf.get_file_name() + 'mime type is None')
                else:
                    if cdf.get_mime_type().startswith('text'):
                        logger.debug('Success, but type is text: ' +
                                     cdf.get_file_name())

        if cdf.get_mime_type() not in mimetypes:
            mimetypes[cdf.get_mime_type()] = 1
        else:
            mimetypes[cdf.get_mime_type()] += 1

    if theargs.printfailed is True:
        sys.stdout.write('-----------\n')

    num_unique_ids = len(unique_ids.keys())
    num_failed_ids = len(failed_id_hash.keys())
    sys.stdout.write('\n')
    sys.stdout.write('Number entries: ' + str(counter) +
                     ' (failed: ' + str(failed_count) + ')\n')
    sys.stdout.write('Number unique IDs: ' + str(num_unique_ids) +
                     ' (failed: ' + str(num_failed_ids) + ')\n')

    sys.stdout.write('Number entries that are NOT supposed to have '
                     'raw file: ' +
                     str(not_supposed_to_have_raw) + '\n')
    sys.stdout.write('-----------------\n')
    for entry in mimetypes.keys():
        sys.stdout.write('\t' + str(entry) + ' ==> ' +
                         str(mimetypes[entry]) + '\n')
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
    except Exception:
        logger.exception('Caught fatal exception')
        return 1


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
