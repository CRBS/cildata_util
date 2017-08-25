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
from cildata_util.dbutil import CILDataFileFromDatabaseFactory

logger = logging.getLogger('cildata_util.cildatacheckdownload')


def _parse_arguments(desc, args):
    """Parses command line arguments
    """
    help_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=help_formatter)

    parser.add_argument("datasetfolder", help='Directory containing'
                                              'single dataset')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument("--dryrun", action='store_true',
                        help='If set, script will just'
                             'print what it will do to'
                             'standard out, but not perform'
                             'download or update of json')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + cildata_util.__version__))
    return parser.parse_args(args)


def _find_json_file(dataset_dir):
    """Searches dataset_dir for file ending with JSON_SUFFIX
       and returns first one encountered.
    """
    abspath = os.path.abspath(dataset_dir)
    for entry in os.listdir(dataset_dir):
        if entry.endswith(dbutil.JSON_SUFFIX):
            return os.path.join(abspath, entry)
    return None


def _make_backup_of_json(jsonfile):
    cntr = 0
    backupfile = jsonfile + '.bk.' + str(cntr)
    while os.path.isfile(backupfile):
        backupfile = jsonfile + '.bk.' + str(cntr)
        cntr += 1
    shutil.copy(jsonfile, backupfile)


def _add_missing_cildatafile_objects(cdf_list):
    """Adds any missing CILDataFile objects
    """
    if cdf_list is None:
        logger.error('cdf_list is None')
        return None

    if len(cdf_list) is 0:
        logger.error('cdf_list is empty dont know if its a video'
                     'or image')
        return None

    suffix_dict = {}
    if cdf_list[0].get_is_video():
        for entry in CILDataFileFromDatabaseFactory.VID_SUFFIX_LIST:
            suffix_dict[entry] = 0
    else:
        for entry in CILDataFileFromDatabaseFactory.IMG_SUFFIX_LIST:
            suffix_dict[entry] = 0

    for cdf in cdf_list:
        suffix = re.sub("^.*\.", ".", cdf.get_file_name())
        suffix_dict[suffix] += 1

    for k in suffix_dict.keys():
        if suffix_dict[k] is 0:
            logger.info('Missing CILDataFile for ' + k + ' adding')
            newcdf = CILDataFile(cdf_list[0].get_id())
            newcdf.set_file_name(str(cdf_list[0].get_id()) + k)
            cdf_list.append(newcdf)

    return cdf_list


def _checkdownload(theargs):

    if theargs.dryrun is True:
        sys.stdout.write('DRY RUN mode, no changes will be made\n')

    destpath = os.path.abspath(theargs.datasetfolder)
    jsonfile = _find_json_file(destpath)
    reader = CILDataFileListFromJsonPickleFactory()
    cdf_list = reader.get_cildatafiles(jsonfile)
    cdf_list = _add_missing_cildatafile_objects(cdf_list)
    newcdf_list = []
    updated = False
    for cdf in cdf_list:
        if not os.path.isfile(os.path.join(destpath, cdf.get_file_name())):
            logger.info(cdf.get_file_name() + ' file not found. Downloading...')
            if theargs.dryrun is True:
                sys.stdout.write('DRY RUN: Download' + cdf.get_file_name() +'\n')
            else:
                newcdf = dbutil.download_cil_data_file(destpath, cdf,
                                                       loadbaseurl=not updated,
                                                       download_direct_to_dest=True)
                updated = True
                newcdf_list.append(newcdf)

        else:
            newcdf_list.append(cdf)

    if updated is True and theargs.dryrun is False:
        logger.info('Making backup of json and writing updated version of json')
        _make_backup_of_json(jsonfile)
        writer = CILDataFileJsonPickleWriter()
        writer.writeCILDataFileListToFile(jsonfile,
                                          newcdf_list,
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

              This program examines a directory with a downloaded
              dataset (ID.json, .raw etc..) and downloads
              any missing files and updates ID.json appropriately
              the original json is backed up with ID.json.bk
              or ID.json.bk.# if file exists where # is next
              available number.
    """.format(version=cildata_util.__version__)

    theargs = _parse_arguments(desc, args[1:])
    theargs.program = args[0]
    theargs.version = cildata_util.__version__
    config.setup_logging(logger, loglevel=theargs.loglevel)

    try:
        return _checkdownload(theargs)
    except Exception as e:
        logger.exception('Caught fatal exception')
        return 1


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
