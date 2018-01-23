#! /usr/bin/env python


import argparse
import sys
import logging
import os
import re
from PIL import Image

import cildata_util
from cildata_util import config
from cildata_util import dbutil


logger = logging.getLogger('cildata_util.cildatathumbnailcreator')

SIZES_FLAG = '--sizes'
SUFFIX_FLAG = '--suffix'

THUMBNAIL_LABEL = '_thumbnailx'


def _parse_arguments(desc, args):
    """Parses command line arguments
    """
    help_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=help_formatter)
    parser.add_argument("downloaddir",
                        help='Directory where images and videos reside')
    parser.add_argument("destdir",
                        help='Directory where thumbnails will be saved')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument(SIZES_FLAG,
                        default='88,140,220,512',
                        help='Comma delimited list of thumbnail sizes'
                             'in pixels. The thumbnail will maintain'
                             ' aspect ratio and be padded for'
                             ' non square images with black. '
                             '(default 88,140,220,512)')
    parser.add_argument(SUFFIX_FLAG, default = '.jpg',
                        help='Suffix for images. (default .jpg)')
    parser.add_argument('--id', help='Only convert data with id passed in.')
    parser.add_argument('--overwrite', action='store_true',
                        help='If set overwrites any existing thumbnails. '
                             'Otherwise existing thumbnails are skipped.')
    parser.add_argument('--dryrun', action='store_true',
                        help='If set, simply output via logging at INFO level '
                             'what thumbnails will be created.')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + cildata_util.__version__))
    return parser.parse_args(args)


def _create_thumbnail_images(image_file, size_list, abs_destdir):
    """
    Given an image file this function generates a set of thumbnail
    images using sizes in size_list and saving results in abs_destdir
    with naming convention:

    <image file name - suffix>_thumbnailx<size>.<suffix>

    :param image_file: Path to input image file
    :param size_list: List of desired thumbnail image sizes
    :param abs_destdir: Base destination directory
    :return: 0 upon otherwise 1 or greater for failure
    """
    # TODO Split this function into smaller pieces
    try:
        im = Image.open(image_file)
    except IOError as e:
        logger.exception('Caught exception')
        return 1

    image_name = os.path.basename(image_file)
    thumbprefix = re.sub('\.*$', '', image_name)
    suffix = '.' + re.sub('^.*\.', '', image_name)
    for cursize in size_list:
        thumby_img = Image.new(im.mode,(cursize, cursize))
        im_copy = im.copy()
        im_copy.thumbnail((cursize, cursize), Image.LANCZOS)
        paste_x = 0
        paste_y = 0
        if im_copy.size[0] < cursize:
            paste_x = int((cursize - im_copy.size[0])/2)
        if im_copy.size[1] < cursize:
            paste_y = int((cursize - im_copy.size[1])/2)
        thumby_img.paste(im_copy, (paste_x, paste_y))
        dest_subdir = os.path.join(abs_destdir, thumbprefix)
        if not os.path.isdir(dest_subdir):
            os.makedirs(dest_subdir, mode=0o755)
        dest = os.path.join(dest_subdir,
                            thumbprefix + THUMBNAIL_LABEL + str(cursize) +
                            suffix)
        thumby_img.save(dest)
        im_copy.close()
        thumby_img.close()
    im.close()

    return 0


def _create_thumbnails(theargs):
    """Examine all downloaded data and retry any
       failed entries
    """

    abs_input = os.path.abspath(theargs.downloaddir)
    abs_destdir = os.path.abspath(theargs.destdir)
    raw_size_list = theargs.sizes.split(',')
    size_list = []

    for entry in raw_size_list:
        entry.append(int(entry))

    if os.path.isfile(abs_input):
        logger.debug('Input is a file: ' + abs_input)
        return _create_thumbnail_images(abs_input, size_list, abs_destdir)

    if not os.path.isdir(abs_input):
        logger.error('Expected a directory, but didnt get one')
        return 1

    images_destdir = os.path.join(abs_input, dbutil.IMAGES_DIR)
    videos_destdir = os.path.join(abs_input, dbutil.VIDEOS_DIR)

    return 0


def main(args):
    """Main entry into cildatathumbnailcreator.py
    :param args: should be set to sys.argv aka the list of arguments
                 starting with script name as first argument
    :returns: exit code of 0 upon success otherwise failure
    """

    desc = """
              Version {version}

              This script generates thumbnail images of
              files with suffix specified by {suffix} flag
              in sizes specified by {sizes} flag.

              This script requires two arguments in this order: 
              
              <INPUT> <DEST>
              
              <INPUT> should be an input directory 
              or image file.
              
              <DEST> is destination directory that will be 
              created if it does not already exist.
              
              What data is converted depends on the
              the layout of data in the <INPUT> argument. 
              
              If the <INPUT> is an image file:
              
              A thumbnail for each size in {sizes} flag will 
              be created with file name convention described 
              below in 'Output thumbnail naming convention' section 
              below.
              
              If the <INPUT> is a directory:
              
              If within the directory an {images}/ and/or {videos}/ 
              directories are found then the following occurs
              for each of those directories:
              
              1) A {images} and/or {videos} subdirectory is created
                 under <DEST> directory.
                 
              2) The script examines the {images} and/or {videos} 
                 directories and looks for every directory named with
                 a number ### ie 1235. 
                 
              3) Thumbnails are created for any image file within those
                 numeric directories if the image file is named with 
                 same numeric name as parent directory with suffix 
                 matching {suffix} flag value. The resulting thumbnails
                 are put into the <DEST> directory under
                 a directory with same numeric ### name:
                    
                 <DEST>/####/
                                  
                 Example:
                  
                 If <INPUT> has an {images}/ directory and
                 within is a directory named 12345 and it contains
                 the image file 12345.jpg, the following thumbnails
                 would be created:
                 
                 <DEST>/12345/
                              12345_thumbnailx88.jpg 
                              12345_thumbnailx140.jpg 
                              12345_thumbnailx220.jpg
                              12345_thumbnailx512.jpg 
                              
              In addition if the <INPUT> is a directory any
              directories immediately under the <INPUT> directory
              that are numeric ie 12345 will also have thumbnails
              generated as described above for {images} and {videos}
              sub directories. 
              
              WARNING: File name conflict can occur if the same
                       directory exists in <DEST> and/or {images}/, 
                       {videos}/ directories. This would result in
                       thumbnail files being overwritten.

              Output thumbnail naming convention:
              
              <ORIG FILE -SUFFIX>_thumbnailx<SIZE>.<SUFFIX> 
              
              <ORIG FILE -SUFFIX> -- original file name minus suffix
              
              <SIZE> -- size of thumbnail in pixels ie 88.
              
              <SUFFIX> -- suffix specified with {suffix} flag.
              
              Example:
              
              For input of 12345.jpg would thumbnail named
              
              12345_thumbnailx512.jpg
              
              
              For more information visit:

              https://github.com/slash-segmentation/CIL_file_download_tool/wiki
    """.format(images=dbutil.IMAGES_DIR,
               videos=dbutil.VIDEOS_DIR,
               sizes=SIZES_FLAG,
               suffix=SUFFIX_FLAG,
               version=cildata_util.__version__)

    theargs = _parse_arguments(desc, args[1:])
    theargs.program = args[0]
    theargs.version = cildata_util.__version__
    config.setup_logging(logger, loglevel=theargs.loglevel)

    try:
        return _create_thumbnails(theargs)
    except Exception:
        logger.exception('Caught fatal exception')
        return 1


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
