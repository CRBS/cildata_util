#! /usr/bin/env python


import argparse
import sys
import logging
import os
import zipfile

import cildata_util
from cildata_util import config
from cildata_util import dbutil
from cildata_util.dbutil import CILDataFileJsonPickleWriter
from cildata_util.dbutil import CILDataFileFromJsonFilesFactory
from cildata_util.dbutil import CILDataFileListFromJsonPickleFactory
from cildata_util.dbutil import CILDataFileConverter
from cildata_util.dbutil import CILDataFileNoRawFilter

logger = logging.getLogger('cildata_util.cildataconverter')


def _parse_arguments(desc, args):
    """Parses command line arguments
    """
    help_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=help_formatter)
    parser.add_argument("downloaddir",
                        help='Directory where images and videos reside')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument('--id', help='Only convert data with id passed in.')
    parser.add_argument('--onlycheckzipfiles', action='store_true',
                        help='If set examines all the zip files in images'
                             'and reports number of files and file names'
                             'found.')
    parser.add_argument('--skipifrawmissing', action='store_true',
                        help='If set skip any ids where no raw file is found'
                             'in directory.')

    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + cildata_util.__version__))
    return parser.parse_args(args)


def _check_zip_files(cdf, base_dir):
    if cdf.get_is_video() is True:
        return
    if not cdf.get_file_name().endswith(dbutil.RAW_SUFFIX):
        return

    zfile = os.path.join(base_dir, cdf.get_file_name())
    if not zipfile.is_zipfile(zfile):
        sys.stdout.write(zfile + ' is NOT a zip file!!!\n')
        return
    zf = zipfile.ZipFile(zfile, mode='r', allowZip64=True)
    zentries = zf.infolist()

    if len(zentries) != 1:
        sys.stdout.write(zfile + '\n----------\n')
        sys.stdout.write('\t' + zfile + ' has ' + str(len(zentries)) +
                         ' entries!!!\n')
        for entry in zentries:
            sys.stdout.write('\t' + entry.filename + '\n')

        sys.stdout.write('\n')
    zf.close()


def _convert_data(theargs):
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

    converter = CILDataFileConverter()
    reader = CILDataFileListFromJsonPickleFactory()
    writer = CILDataFileJsonPickleWriter()

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

        if theargs.onlycheckzipfiles is True:
            _check_zip_files(cdf, base_dir)
            continue

        if theargs.skipifrawmissing is True:
            raw = os.path.join(base_dir, str(cdf.get_id() + dbutil.RAW_SUFFIX))
            if not os.path.isfile(raw):
                logger.debug('Skipping... ' + str(cdf.get_id()) + ' no raw file found')
                continue

        newcdfs = converter.convert(cdf, base_dir)

        jsonfile = os.path.join(base_dir, str(cdf.get_id()) + dbutil.JSON_SUFFIX)
        logger.debug('Making backup of ' + jsonfile)
        dbutil.make_backup_of_json(jsonfile)

        cdf_list = reader.get_cildatafiles(jsonfile)

        newcdf_list = []
        for entry in cdf_list:
            logger.debug('Entry file name: ' + entry.get_file_name() +
                         ' and cdf file name: ' + cdf.get_file_name())

            if entry.get_file_name() == cdf.get_file_name():
                logger.info('Updating ' + cdf.get_file_name())
                if isinstance(newcdfs, list):
                    logger.debug('Extending list')
                    newcdf_list.extend(newcdfs)
                else:
                    logger.debug('Appending to list')
                    newcdf_list.append(newcdfs)
                continue
            newcdf_list.append(entry)

        writer.writeCILDataFileListToFile(jsonfile, newcdf_list,
                                          skipsuffixappend=True)

    return 0


def main(args):
    """Main entry into cildataconverter
    :param args: should be set to sys.argv aka the list of arguments
                 starting with script name as first argument
    :returns: exit code of 0 upon success otherwise failure
    """

    desc = """
              Version {version}

              Given a directory of images and videos downloaded by
              cildatadownloader.py, this script performs a set of
              file renames and conversions on each dataset found within.

              The only required argument is the output directory
              generated by cildatadownloader.py which should contain
              two subdirectories images/ and videos/.


              IMAGES:

              For datasets under images/ directory the following
              conversions are performed:

              <ID>.raw is examined and verified to be a valid zip file.

              Any files found in <ID>.raw are extracted and written
              to <ID> directory in format <ID>_orig.<FORMAT> where
              <FORMAT> is the extension found in the <ID>.zip file.

              <ID>.zip is created with the following structure:
                 <ID>/
                      <ID>_orig.<FORMAT>
                      <ID>_orig.<FORMAT>

              If the zip file looks to exceed 4gb 64bit extensions
              are enabled.

              The json file is updated to have correct md5 checksums,
              file sizes, and mime_types as determined by headers
              when downloading and/or by file extension.

              VIDEOS:

              For datasets under videos/ directory the following
              conversions are performed:

              <ID>.raw is renamed based on its mimetype obtained
              from http headers when downloaded and named
              <ID>.<MIMETYPE SUFFIX>.

              <ID>.zip file is created with following structure
                 <ID>/
                      <ID>.<MIMETYPE SUFFIX>

              The json file is updated to have correct md5 checksums,
              file sizes, and mime_types as determined by headers
              when downloading and/or by file extension.

              For more information visit:

              https://github.com/slash-segmentation/cildata_util/wiki
    """.format(version=cildata_util.__version__)

    theargs = _parse_arguments(desc, args[1:])
    theargs.program = args[0]
    theargs.version = cildata_util.__version__
    config.setup_logging(logger, loglevel=theargs.loglevel)

    try:
        return _convert_data(theargs)
    except Exception:
        logger.exception('Caught fatal exception')
        return 1


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
