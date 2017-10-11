import re
import os
import logging
import pg8000
import jsonpickle
import json
import hashlib
import shutil
import requests
import time
import mimetypes
import zipfile
from dateutil import parser

logger = logging.getLogger(__name__)

IMAGES_DIR = 'images'
VIDEOS_DIR = 'videos'
JSON_SUFFIX = '.json'
BK_TXT = '.bk.'
RAW_SUFFIX = '.raw'
JPG_SUFFIX = '.jpg'
TIF_SUFFIX = '.tif'
FLV_SUFFIX = '.flv'
ZIP_SUFFIX = '.zip'

ZIP_MIMETYPE = 'application/zip'
ORIG_IDENTIFIER = '_orig'
CONTENT_DISPOSITION = 'Content-disposition'


def make_backup_of_json(jsonfile):
    """Makes copy of file by appending .bk.# where
       # is one whole number higher then the highest
       number found in directory.
       :raises IOError: If `jsonfile` does not exist
       :raises OSError: Can also be raised if there is a copy problem
    """
    cntr = 0
    backupfile = jsonfile + BK_TXT + str(cntr)
    while os.path.isfile(backupfile):
        backupfile = jsonfile + BK_TXT + str(cntr)
        cntr += 1
    shutil.copy(jsonfile, backupfile)


def download_file(url, dest_dir, numretries=2,
                  retry_sleep=30, timeout=120,
                  session=None):
    """Downloads file from `url` to `dest_dir` path
    :param url: URL to download ie http://foo.com/file
    :param numretries: Number of retries, default 2
    :param retry_sleep: Seconds to sleep between retries, default 30
    :param timeout: Seconds before timeout, default 120
    :param session: Allows caller to use custom Requests.session to
                    retrieve file, default None to use requests.get
    :returns: tuple (local filename, requests.headers, requests.status_code)
    """
    local_filename = url.split('/')[-1]
    dest_file = os.path.join(dest_dir, local_filename)
    logger.debug('Downloading from ' + url + ' to ' + dest_file)
    retry_count = 0
    while retry_count <= numretries:
        try:
            if session is not None:
                logger.debug('Using custom session object for get')
                r = session.get(url, timeout=timeout, stream=True)
            else:
                r = requests.get(url, timeout=timeout, stream=True)

            with open(dest_file, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
            logger.debug('Headers: ' + str(r.headers))
            if r.status_code is not 200:
                retry_count += 1
                logger.error('Got ' + str(r.status_code) + ' trying to '
                             'download. Sleeping ' + str(retry_sleep) +
                             ' seconds and will retry again # ' +
                             str(retry_count))
                time.sleep(retry_sleep)
                continue
            return local_filename, r.headers, r.status_code
        except Exception as e:
            retry_count += 1
            logger.exception('Caught some exception trying to '
                             'download. Sleeping ' + str(retry_sleep) +
                             ' seconds and will retry again # ' +
                             str(retry_count))
            time.sleep(retry_sleep)

    return None, None, 999


def get_download_url(base_url, omero_url, cdf):

    if cdf is None:
        logger.error('cdf is None cannot get download url')
        return None

    if base_url is None:
        logger.error('base_url is None cannot get download url')
        return None

    if omero_url is None:
        logger.error('omero_url is None cannot get download url')
        return None

    if cdf.get_file_name() is None:
        logger.error('Unable to get cdf aka CILDataFile filename'
                     'cannot figure out what to download')
        return None

    if cdf.get_file_name().endswith(FLV_SUFFIX):
        return base_url + 'videos/'

    if cdf.get_file_name().endswith(JPG_SUFFIX):
        return base_url + 'images/download_jpeg/'

    if cdf.get_file_name().endswith(TIF_SUFFIX):
        return omero_url

    if cdf.get_file_name().endswith(RAW_SUFFIX):
        return omero_url

    logger.error('Not sure how to download this file: ' +
                 cdf.get_file_name())
    return None


def md5(fname):
    """Calculates md5 hash on file passed in
    :param fname: file to examine
    :returns: None if no file is passed in or if file does
              not exist otherwise md5hash hexdigest()
    """
    if fname is None:
        return None

    if not os.path.isfile(fname):
        logger.error(fname + ' is not a file')
        return None

    hash_md5 = hashlib.md5()
    logger.debug('Calculating md5 of file: ' + fname)
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def convert_response_headers_to_dict(headers):
    """Converts Requests.Response headers to
       dictionary
    """
    if headers is None:
        logger.error('Headers is None')
        return None

    header_dict = {}

    for k in headers.keys():
        header_dict[k] = headers[k]
    return header_dict


def download_cil_data_file(destination_dir, cdf, loadbaseurl=False,
                           download_direct_to_dest=False,
                           numretries=2,retry_sleep=30, timeout=120):

    base_url = 'http://www.cellimagelibrary.org/'
    omero_url = 'http://grackle.crbs.ucsd.edu:8080/OmeroWebService/images/'

    if cdf is None:
        logger.error('cdf is None cannot download')
        return None

    if download_direct_to_dest is False:
        out_dir = os.path.join(destination_dir, str(cdf.get_id()))
    else:
        out_dir = destination_dir

    if not os.path.isdir(out_dir):
        os.makedirs(out_dir, mode=0o755)
    if loadbaseurl is True:
        # hit download page
        logger.debug('Loading base url')
        aurl = base_url + 'images/' + str(cdf.get_id())
        r = requests.get(aurl)
        if r.status_code is not 200:
            logger.warning('Hitting ' + aurl + ' returned status of ' +
                           str(r.status_code))

    download_url = get_download_url(base_url, omero_url, cdf)
    if download_url is None:
        return None

    logger.info('Downloading file: ' + cdf.get_file_name())
    (local_file, headers,
     status) = download_file(download_url +
                             cdf.get_file_name(),
                             out_dir,
                             numretries=numretries,
                             retry_sleep=retry_sleep,
                             timeout=timeout)

    logger.debug('status: ' + str(status))
    if headers is not None:
        logger.debug('content type: ' + headers['Content-Type'])
        cdf.set_mime_type(headers['Content-Type'])
        cdf.set_localfile(local_file)
        cdf.set_headers(convert_response_headers_to_dict(headers))
    if status is 200:
        cdf.set_download_success(True)
        local_file_fp = os.path.join(out_dir, cdf.get_localfile())
        cdf.set_checksum(md5(local_file_fp))
        cdf.set_file_size(os.path.getsize(local_file_fp))
    else:
        logger.warning('Error downloading ' + cdf.get_file_name() +
                       ' code: ' + str(status))
        cdf.set_download_success(False)

    return cdf


class Database(object):
    """Gets connection to database using `CILDatabaseConfig` passed into
       constructor
    """
    def __init__(self, cil_database_config):
        """Constructor
        :param cil_database_config: valid CILDatabaseConfig object
        """
        self._config = cil_database_config
        self._alt_conn = None

    def set_alternate_connection(self, conn):
        """Sets alternate connection to be returned by get_connection()
           used for testing purposes
        :param conn: alternate connection to return
        """
        self._alt_conn = conn

    def get_connection(self):
        """Gets connection to database
        :returns: Connection to database as Connection object
        """
        if self._alt_conn is not None:
            logger.info("Using alternate database connection")
            return self._alt_conn

        logger.debug('Getting database connection to pg8000')
        conn = pg8000.connect(host=self._config.get_host(),
                              user=self._config.get_user(),
                              password=self._config.get_password(),
                              port=int(self._config.get_port()),
                              database=self._config.get_database_name())
        return conn


class CILDataFileDatabaseUpdater(object):
    """Inserts CILDataFile entries into database"""
    def __init__(self, conn):
        """Constructor
        :param conn: Database connection already connected
        """
        self._conn = conn

    def insert_cildatafiles(self, cildatafile_list):
        """Inserts CILDataFile objects in cildatafile_list
        into database.
        :param cildatafile_list: list of CILDataFile objects
        """
        cursor = self._conn.cursor()
        try:
            for cdf in cildatafile_list:
                logger.debug('Inserting ' + str(cdf.get_id()))
                if cdf.get_is_video() is None:
                    logger.debug('Setting is_video() to False cause '
                                 'is_video() is set to None')
                    cdf.set_is_video(False)
                if cdf.get_file_name().endswith(JPG_SUFFIX):
                    logger.debug('Setting is_video() to False cause '
                                 'file is jpg')
                    cdf.set_is_video(False)
                thedatestr = 'now()'

                if cdf.get_mime_type() is None:
                    logger.debug('mime type was None setting to '
                                 'application/octet-stream')
                    cdf.set_mime_type('application/octet-stream')

                if cdf.get_file_size() is None:
                    logger.debug('Checksum is None setting to 0')
                    cdf.set_file_size(0)
                try:
                    dt = parser.parse(cdf.get_headers()['Date'])
                    thedatestr = "'" + dt.strftime('%Y-%m-%d %I:%M:%S') + "'"
                except ValueError as e:
                    logger.error('ValueError: Unable to get date from header: ' + str(e))
                except TypeError as et:
                    logger.error('TypeError: Unable to get date from header: ' + str(et))

                insert_str = ("INSERT INTO cil_download_status(id,image_id," +
                              "is_video,file_name,download_success," +
                              "download_time,checksum,checksum_value,mime_type," +
                              "num_of_bytes) " +
                              "VALUES(nextval('cil_downloader_seq')," +
                              str(cdf.get_id()) + "," +
                              str(cdf.get_is_video()) + ",'" +
                              cdf.get_file_name() + "'," +
                              str(cdf.get_download_success()) + "," + thedatestr + ",True,'" +
                              str(cdf.get_checksum()) + "','" +
                              cdf.get_mime_type() + "'," +
                              str(cdf.get_file_size()) + ")")
                logger.debug(insert_str)
                cursor.execute(insert_str)
        finally:
            cursor.close()
            self._conn.commit()


class CILDataFile(object):
    """Represents an image or video file from CIL
    """
    def __init__(self, id):
        """Constructor
        :param id: database identifier for data file
        :param is_video: boolean where True means its a video, False means image
        """
        self._id = id
        self._is_video = None
        self._mimetype = None
        self._file_name = None
        self._download_success = None
        self._download_time = None
        self._checksum = None
        self._localfile = None
        self._headers = None
        self._file_size = None
        self._has_raw = None

    def copy(self, cdf):
        """Copies all values of CILDataFile passed in
        :param cdf: CILDataFile object to copy
        """
        try:
            self._is_video = cdf._is_video
        except AttributeError:
            pass
        try:
            self._mimetype = cdf._mimetype
        except AttributeError:
            pass
        try:
            self._file_name = cdf._file_name
        except AttributeError:
            pass
        try:
            self._download_success = cdf._download_success
        except AttributeError:
            pass
        try:
            self._download_time = cdf._download_time
        except AttributeError:
            pass
        try:
            self._checksum = cdf._checksum
        except AttributeError:
            pass
        try:
            self._localfile = cdf._localfile
        except AttributeError:
            pass
        try:
            self._headers = cdf._headers
        except AttributeError:
            pass
        try:
            self._file_size = cdf._file_size
        except AttributeError:
            pass
        try:
            self._has_raw = cdf._has_raw
        except AttributeError:
            pass

    def get_id(self):
        """Gets id
        """
        return self._id

    def set_is_video(self, is_video):
        """Sets is video
        """
        self._is_video = is_video

    def set_mime_type(self, mimetype):
        """Sets mime type
        """
        self._mimetype = mimetype

    def set_file_name(self, file_name):
        """Sets filename
        """
        self._file_name = file_name

    def set_download_success(self, success_val):
        """Sets whether download is successful
        """
        self._download_success = success_val

    def set_download_time(self, download_time):
        """Sets download time
        """
        self._download_time = download_time

    def set_checksum(self, checksum):
        """Sets checksum of downloaded file
        """
        self._checksum = checksum

    def set_localfile(self, localfile):
        self._localfile = localfile

    def set_headers(self, headers):
        """Sets headers obtained when
           downloading data
        """
        self._headers = headers

    def set_file_size(self, size_in_bytes):
        """Sets size of file in bytes
        """
        self._file_size = size_in_bytes

    def set_has_raw(self, has_raw_val):
        """Sets has_raw
        """
        self._has_raw = has_raw_val

    def get_has_raw(self):
        """Gets has_raw
        """
        return self._has_raw

    def get_file_size(self):
        """Gets size of file in bytes
        """
        return self._file_size

    def get_headers(self):
        """Gets headers obtained when
           downloading data
        """
        return self._headers

    def get_is_video(self):
        """Gets is video
        """
        return self._is_video

    def get_mime_type(self):
        """Gets mime type
        """
        return self._mimetype

    def get_file_name(self):
        """Gets filename
        """
        return self._file_name

    def get_download_success(self):
        """Gets whether download is successful
        """
        return self._download_success

    def get_download_time(self):
        """Gets download time
        """
        return self._download_time

    def get_checksum(self):
        """Gets checksum of downloaded file
        """
        return self._checksum

    def get_localfile(self):
        return self._localfile


class CILDataFileFoundInFilesystemFilter(object):
    """Filter that removes any CILDataFile objects
       that already exist in the file system
    """
    def __init__(self, images_dir, videos_dir):
        """
        COnstructor
        :param images_dir: Directory where images are stored
        :param videos_dir: Directory where videos are stored
        :raises ValueError: If either images_dir or videos_dir is None
        """
        self._images_dir = images_dir
        self._videos_dir = videos_dir
        if self._images_dir is None:
            raise ValueError('images_dir cannot be None')
        if self._videos_dir is None:
            raise ValueError('videos_dir cannot be None')

    def get_cildatafiles(self, cildatafile_list):
        """Filters out any CILDataFile objects
           that have a presence on filesystem
           where presence means a directory
           exists
        :returns: list of CILDataFile objects that passed filter or
                  None if None was passed in. An empty list will return
                  an empty list.
        """

        if cildatafile_list is None:
            logger.debug('Received None so returning None')
            return None

        filtered_cdf_list = []
        for cdf in cildatafile_list:
            if cdf.get_is_video():
                base_dir = self._videos_dir
            else:
                base_dir = self._images_dir

            cdf_dir = os.path.join(base_dir, str(cdf.get_id()))
            if os.path.isdir(cdf_dir):
                continue

            filtered_cdf_list.append(cdf)
        return filtered_cdf_list


class CILDataFileNoRawFilter(object):
    """Filter that removes any CILDataFile image objects
       that end with .raw and whose get_has_raw() is
       set to False
    """
    def __init__(self):
        """
        COnstructor
        """

    def get_cildatafiles(self, cildatafile_list):
        """Filters out any CILDataFile objects as described
           in constructor.
        :raises AttributeError: if CILDataFile does not have values for
                                get_file_name()
        :returns: filtered list of CILDataFile objects
        """
        if cildatafile_list is None:
            logger.debug('Received None so returning None')
            return None

        filtered_cdf_list = []
        for cdf in cildatafile_list:
            if cdf.get_is_video() is not True:
                if cdf.get_file_name().endswith(RAW_SUFFIX):
                    if cdf.get_has_raw() is False:
                        logger.debug('Skipping entry: ' + cdf.get_file_name())
                        continue
            filtered_cdf_list.append(cdf)
        return filtered_cdf_list


class CILDataFileFailedDownloadFilter(object):
    """Filter that retreives CILDataFile objects that
       failed to download, excluding .raw image
       entries that failed to download with get_has_raw()
       set to False
    """
    def __init__(self):
        """Constructor"""

    def get_cildatafiles(self, cildatafile_list):
        """Removes CILDataFile objects that
        had a successful download or if they were
        .raw image entries that failed to download
         with get_has_raw() set to False
        """
        if cildatafile_list is None:
            logger.error('Received None so returning None')
            return None
        filtered_cdf_list = []
        for cdf in cildatafile_list:
            if cdf.get_download_success() is True:
                continue
            filtered_cdf_list.append(cdf)
        return filtered_cdf_list


class CILDataFileFromJsonFilesFactory(object):
    """Generates CILDataFile objects by parsing
       json files found in directory passed in.
    """
    def __init__(self):
        """Constructor
        """

    def _get_all_json_files(self, path):
        """Generator
        """
        # logger.debug('Examining ' + path)
        if os.path.isfile(path) and path.endswith(JSON_SUFFIX):
            # logger.debug('Yielding ' + path)
            yield path
        if os.path.isdir(path):
            for entry in os.listdir(path):
                fp = os.path.join(path, entry)
                for subentry in self._get_all_json_files(fp):
                    yield subentry

    def get_cildatafiles(self, dir_path):

        if dir_path is None:
            logger.error('None passed in')
            return None

        reader = CILDataFileListFromJsonPickleFactory()
        full_list = []
        for jsonfile in self._get_all_json_files(dir_path):
            for entry in reader.get_cildatafiles(jsonfile):
                full_list.append(entry)
        return full_list


class CILDataFileFromDatabaseFactory(object):
    """Obtains CILDataFile objects from database
    """
    IMG_SUFFIX_LIST = [TIF_SUFFIX, JPG_SUFFIX, RAW_SUFFIX]
    VID_SUFFIX_LIST = [FLV_SUFFIX, RAW_SUFFIX, JPG_SUFFIX]

    def __init__(self, conn, id=None, skipifrawfalse=False):
        """Constructor
        :param conn: database connection already connected
        :param id: only return CILDataFile objects with matching id.
        :param skipifrawfalse: Omit the .raw image if has_raw is False
        """
        self._conn = conn
        self._id = id
        self._skipifrawfalse = skipifrawfalse

    def get_cildatafiles(self):
        """Queries database to get CILDataFile objects
           creating a duplicate entry for each image
           type (tif, raw, jpg)
           method also updates status information from status table
        """
        origcdflist = self._get_cildatafiles_from_data_type_table()
        return self._generate_cildatafiles_from_database(origcdflist)

    def _generate_cildatafiles_from_database(self, cdflist):
        """Given a list of CILDataFile objects from the database,
        which is one CILDataFile per id, generates all the CILDataFile
        objects which should include multiple CILDataFile objects
        per id.
        :param cdflist:
        :return:
        """
        newcdflist = []
        for cdf in cdflist:
            if cdf.get_is_video():
                counter = 0
                cur_id = cdf.get_id()
                for suffix in CILDataFileFromDatabaseFactory.VID_SUFFIX_LIST:
                    if counter is 0:
                        newcdf = cdf
                        counter = 1
                    else:
                        newcdf = CILDataFile(cur_id)
                        newcdf.copy(cdf)
                        newcdf.set_is_video(True)

                    newcdf.set_file_name(str(cur_id) + suffix)
                    newcdflist.append(newcdf)
            else:
                counter = 0
                cur_id = cdf.get_id()
                for suffix in CILDataFileFromDatabaseFactory.IMG_SUFFIX_LIST:
                    if self._skipifrawfalse is True and suffix == RAW_SUFFIX:
                        if cdf.get_has_raw() is False:
                            continue

                    if counter is 0:
                        newcdf = cdf
                        counter = 1
                    else:
                        newcdf = CILDataFile(cur_id)
                    newcdf.copy(cdf)
                    newcdf.set_file_name(str(cur_id) + suffix)
                    newcdflist.append(newcdf)
        return newcdflist

    def _update_CILDataFileWithStatusFromDatabase(self, cdf):
        """Queries status table with cdf to get any info
        """
        return cdf

    def _get_cildatafiles_from_data_type_table(self):
        """Queries database to generate CILDataFile objects"""
        cildatafiles = []
        cursor = self._conn.cursor()
        try:
            if self._id is not None:
                idfilter = " AND image_id='CIL_" + self._id + "'"
            else:
                idfilter = ''
            cursor.execute("SELECT replace(image_id,'CIL_', '') as image_id,"
                           "is_video, has_raw from cil_data_type where is_public=true" +
                           idfilter)

            for entry in cursor.fetchall():
                cdf = CILDataFile(entry[0])
                cdf.set_is_video(bool(entry[1]))
                cdf.set_has_raw(bool(entry[2]))

                cildatafiles.append(cdf)
        finally:
            cursor.close()
            self._conn.commit()

        return cildatafiles


class CILDataFileJsonPickleWriter(object):
    """Persists CILDataFile objects to a file using jsonpickle
    """
    def __init__(self):
        """Constructor
        """
        pass

    def writeCILDataFileListToFile(self, outfile,
                                   cildatafile_list,
                                   skipsuffixappend=False):

        """Writes CILDataFile objects in list to a file
           in json format
        """
        json_cdf_list = []
        for cdf in cildatafile_list:
            json_cdf_list.append(jsonpickle.encode(cdf))

        logger.debug('Writing out json file to ' + outfile)
        if skipsuffixappend is False:
            full_outfile = outfile + JSON_SUFFIX
        else:
            full_outfile = outfile

        with open(full_outfile, 'w') as out_file:
            json.dump(json_cdf_list, out_file)
            out_file.flush()


class CILDataFileListFromJsonPickleFactory(object):
    """Factory class that creates CILDataFile objects
       by reading json pickle file
    """
    def __init__(self):
        """Constructor
        """

    def get_cildatafiles(self, json_pickle_file):
        """Gets list of CILDataFile objects from json
           pickle file that is expected to contain
           a list of CILDataFile objects
        :param json_pickle_file:
        :return: list of CILDataFile objects
        """

        with open(json_pickle_file, 'r') as in_file:
            data = in_file.read()
            # this is a hack fix since CaseInsensitiveDict and
            # OrderedDict objects within the urllib3 package
            # is not decoding in jsonpickle
            data = data.replace('requests.packages.urllib3.packages.ordered_dict',
                                'collections')
            data = data.replace('requests.structures',
                                'collections')
            json_cdf_list = json.loads(data)

        cdf_list = []

        for e in json_cdf_list:
            tmpcdf = jsonpickle.decode(e)
            cdf = CILDataFile(tmpcdf.get_id())
            cdf.copy(tmpcdf)
            cdf_list.append(cdf)
        return cdf_list


class CILDataFileConverter(object):
    """Following guidelines set in
    https://github.com/slash-segmentation/CIL_file_download_tool/wiki
    Instances of this class perform renames and create new CILDataFile
    objects and physical files.
    """
    def __init__(self):
        """Constructor
        """

    def convert(self, cdf, cdf_dir):
        """Converts CILDataFile object and corresponding file
        to appropriate format. This method also creates
        new CILDataFile objects if needed.

        If CILDataFile is_video is True then the following occurs:

          If extension on get_file_name() is anything other then
          .raw then CILDataFile is returned unchanged.

          If .raw then the type of file is determined and the
          .raw extension is replaced with this new extension on
          the physical file and in get_file_name(). In addition,
          This new file is placed into a new .zip file and a
          new CILDataFile object is created to represent this .zip
          file.

        If CILDataFile is image aka is_video not True
        (can be None or False) then the following occurs:

          If extension on get_file_name() is .raw, verify its a zip
          file and rename to .zip. Update the CILDataFile with
           this new extension. Then extract contents of this
          .zip file and whatever file found in there should be renamed
          <ID>_orig.<FORMAT> where <FORMAT> is extension already on
          file. A new CILDataFile object should be created for this
          new file and a new md5 run.

        :returns: list of CILDataFile objects
        """
        if cdf is None:
            logger.error('None received so None returned')
            return None

        if not cdf.get_download_success() is True:
            logger.error(str(cdf.get_file_name()) +
                         ' did not have a successful download. Skipping...')
            return cdf

        if cdf.get_file_name() is None:
            raise ValueError('For id ' + str(cdf.get_id()) +
                             ' file name is NOT set')

        if cdf_dir is None:
            raise ValueError('cdf_dir cannot be None')

        if cdf.get_is_video() is not True:
            return self._convert_image(cdf, cdf_dir)

        return self._convert_video(cdf, cdf_dir)

    def _convert_video(self, cdf, cdf_dir):
        """Converts video
        """
        if not cdf.get_file_name().endswith(RAW_SUFFIX):
            return cdf

        # we have a file with .raw ending look at Content-disposition
        # to get suffix and compare that with mimetype.
        new_suffix = self._get_raw_video_extension(cdf)

        # perform rename and update CILDataFile
        cdf = self._change_suffix_on_cildatafile(cdf, new_suffix, cdf_dir)

        # create zip with new file in it.
        zipcdf = self._create_zip_file([cdf], cdf_dir)

        cdf_list = list()
        cdf_list.append(cdf)
        cdf_list.extend(zipcdf)

        return cdf_list

    def _convert_image(self, cdf, cdf_dir):
        """Converts image
        """
        if not cdf.get_file_name().endswith(RAW_SUFFIX):
            return cdf
        old_file = os.path.join(cdf_dir, cdf.get_file_name())

        if not zipfile.is_zipfile(old_file):
            raise ValueError(old_file + ' is NOT a zip file')

        cdf = self._change_suffix_on_cildatafile(cdf, ZIP_SUFFIX,
                                                 cdf_dir, makebackup=True)

        extracted_cdfs = self._extract_image_from_zip(cdf, cdf_dir)
        new_cdf_list = self._create_zip_file(extracted_cdfs,
                                             cdf_dir)
        new_cdf_list.extend(extracted_cdfs)

        if os.path.isfile(old_file):
            logger.debug('Removing: ' + old_file)
            os.unlink(old_file)

        return new_cdf_list

    def _extract_image_from_zip(self, cdf, cdf_dir):
        """Extracts image from zip file specified by CILDataFile `cdf` and
           renames is <ID>_orig.<suffix of file in zip>
        """
        zip_file = os.path.join(cdf_dir, cdf.get_file_name())
        zf = zipfile.ZipFile(zip_file, mode='r', allowZip64=True)
        zipinfo_entries = zf.infolist()
        if len(zipinfo_entries) is 0:
            raise ValueError('Expected at least 1 file in ' + zip_file +
                             ' but found none')

        tmpdir = os.path.join(cdf_dir, 'tmp')
        newcdf_list = []
        try:
            os.makedirs(tmpdir, mode=0o755)
            for zentry in zipinfo_entries:

                extracted_file = zf.extract(zentry, path=tmpdir)
                suffix = re.sub('^.*\.', '', zentry.filename)
                suffix = '.' + suffix.lower()
                new_file_name = str(cdf.get_id()) + ORIG_IDENTIFIER + suffix
                new_file = os.path.join(cdf_dir, new_file_name)
                os.rename(extracted_file, new_file)
                newcdf = CILDataFile(cdf.get_id())
                newcdf.copy(cdf)
                newcdf.set_file_name(new_file_name)
                newcdf.set_localfile(new_file_name)
                newcdf.set_mime_type(mimetypes.guess_type(new_file_name)[0])
                newcdf.set_file_size(os.path.getsize(new_file))
                newcdf.set_checksum(md5(new_file))
                newcdf_list.append(newcdf)
            return newcdf_list
        finally:
            shutil.rmtree(tmpdir)

    def _create_zip_file(self, cdf_list, cdf_dir):
        """Takes file specified in get_file_name() and puts it into
        a zip file named <ID>.zip. If file is > 2gb the zip64 extensions
        will be used.
        """
        zip_file_name = str(cdf_list[0].get_id()) + ZIP_SUFFIX
        dest_zip = os.path.join(cdf_dir, zip_file_name)
        logger.debug('Creating zip file: ' + dest_zip)

        zf = zipfile.ZipFile(dest_zip, mode='w', allowZip64=True)
        newcdf_list = []
        try:
            for cdf in cdf_list:
                vid_file = os.path.join(cdf_dir, cdf.get_file_name())
                arcpath = os.path.join(str(cdf.get_id()),
                                       os.path.basename(vid_file))
                zf.write(vid_file, arcname=arcpath)
        finally:
            zf.close()
        newcdf = CILDataFile(cdf.get_id())
        newcdf.copy(cdf)
        newcdf.set_file_name(zip_file_name)
        newcdf.set_localfile(zip_file_name)
        newcdf.set_mime_type(ZIP_MIMETYPE)
        newcdf.set_file_size(os.path.getsize(dest_zip))
        newcdf.set_checksum(md5(dest_zip))
        newcdf_list.append(newcdf)

        return newcdf_list

    def _change_suffix_on_cildatafile(self, cdf, new_suffix, cdf_dir,
                                      makebackup=False):
        """Changes suffix on CILDataFile by renaming and updating
        object
        :returns CILDataFile: with get_file_name() updated with new suffix
        """
        new_file_name = str(cdf.get_id()) + new_suffix
        new_file = os.path.join(cdf_dir, new_file_name)

        old_file = os.path.join(cdf_dir, cdf.get_file_name())

        if makebackup is True:
            logger.debug('Making copy of ' + old_file + ' naming it ' +
                         new_file)
            shutil.copy(old_file, new_file)
        else:
            logger.debug('Renaming ' + old_file + ' to ' + new_file)
            os.rename(old_file, new_file)

        newcdf = CILDataFile(cdf.get_id())
        newcdf.copy(cdf)
        newcdf.set_file_name(new_file_name)
        newcdf.set_localfile(new_file_name)
        newcdf.set_mime_type(mimetypes.guess_type(new_file_name)[0])
        return newcdf

    def _get_raw_video_extension(self, cdf):
        """hi
        """
        if cdf.get_headers() is None:
            raise ValueError('No headers found for ' +
                             str(cdf.get_file_name()))

        if CONTENT_DISPOSITION not in cdf.get_headers():
            raise ValueError(CONTENT_DISPOSITION + ' NOT in headers for ' +
                             str(cdf.get_file_name()))

        c_disp = cdf.get_headers()[CONTENT_DISPOSITION]
        newsuffix = self._extract_suffix_from_content_disposition(c_disp)

        newsuffix = '.' + newsuffix.lower()
        self._compare_extension_with_mimetype(cdf, newsuffix)
        return newsuffix

    def _extract_suffix_from_content_disposition(self, content_disp):
        """Extracts the suffix found after filename=<FILE>.<SUFFIX>
           in `content_disp` string passed in.
        """
        if content_disp is None:
            raise ValueError('content_disp cannot be None')

        if 'filename=' not in content_disp:
            raise ValueError('filename= not found in ' +
                             CONTENT_DISPOSITION)

        return re.sub('^.*\.', '', content_disp)

    def _compare_extension_with_mimetype(self, cdf, newsuffix):
        """Sees if suffix passed in via `newsuffix` matches
        mimetype.
        """
        if cdf.get_mime_type() is None:
            logger.error('For ' + cdf.get_file_name() + ' mimetype is None')
            return

        guessed_exts = mimetypes.guess_all_extensions(cdf.get_mime_type())

        if newsuffix not in guessed_exts:
            logger.error('For ' + cdf.get_file_name() +
                         ' content-disposition says file is of type: ' +
                         newsuffix +
                         ' but this does not match the mimetype: ' +
                         cdf.get_mime_type())
