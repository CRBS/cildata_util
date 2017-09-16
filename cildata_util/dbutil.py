import os
import logging
import pg8000
import jsonpickle
import json
import hashlib
import shutil
import requests
import time


logger = logging.getLogger(__name__)

JSON_SUFFIX = '.json'
BK_TXT = '.bk.'
RAW_SUFFIX = '.raw'


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
    while retry_count < numretries:
        try:
            if session is not None:
                logger.debug('Using custom session object for get')
                r = session.get(url, timeout=timeout, stream=True)
            else:
                r = requests.get(url, timeout=timeout, stream=True)

            with open(dest_file, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
            logger.debug('Headers: ' + str(r.headers))
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
    try:
        for k in headers.keys():
            header_dict[k] = headers[k]
        return header_dict
    except Exception:
        logger.exception('Caught exception parsing headers')
    return None


def download_cil_data_file(destination_dir, cdf, loadbaseurl=False,
                           download_direct_to_dest=False):

    base_url = 'http://www.cellimagelibrary.org/'
    omero_url = 'http://grackle.crbs.ucsd.edu:8080/OmeroWebService/images/'
    str_id = str(cdf.get_id())

    if download_direct_to_dest is False:
        out_dir = os.path.join(destination_dir, str_id)
    else:
        out_dir = destination_dir

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
    download_url = get_download_url(base_url, omero_url, cdf)
    if download_url is None:
        return

    (local_file, headers,
     status) = download_file(download_url +
                             cdf.get_file_name(),
                             out_dir)

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
        """
        self._images_dir = images_dir
        self._videos_dir = videos_dir

    def get_cildatafiles(self, cildatafile_list):
        """Filters out any CILDataFile objects
           that have a presence on filesystem
           where presence means a directory
           exists
        """
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
        """
        filtered_cdf_list = []
        for cdf in cildatafile_list:
            if cdf.get_is_video() is not True:
                if cdf.get_file_name().endswith(RAW_SUFFIX):
                    if cdf.get_has_raw() is False:
                        logger.debug('Skipping entry: ' + cdf.get_file_name())
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
        pass

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

        reader = CILDataFileListFromJsonPickleFactory()
        full_list = []
        for jsonfile in self._get_all_json_files(dir_path):
            for entry in reader.get_cildatafiles(jsonfile):
                full_list.append(entry)
        return full_list


class CILDataFileFromDatabaseFactory(object):
    """Obtains CILDataFile objects from database
    """
    IMG_SUFFIX_LIST = ['.tif', '.jpg', '.raw']
    VID_SUFFIX_LIST = ['.flv', '.raw', '.jpg']

    def __init__(self, conn, id=None):
        """Constructor
        :param conn: database connection already connected
        :param id: only return CILDataFile objects with matching id.
        """
        self._conn = conn
        self._id = id

    def get_cildatafiles(self):
        """Queries database to get CILDataFile objects
           creating a duplicate entry for each image
           type (tif, raw, jpg)
           method also updates status information from status table
        """
        origcdflist = self._get_cildatafiles_from_data_type_table()
        newcdflist = []
        for cdf in origcdflist:
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
    def __init__(self, fixheaders=False):
        """Constructor
        """
        self._fixheaders = fixheaders

    def _get_fixed_header(self, header):
        """Fixes messed up header object
        """
        if header is None:
            return None
        try:
            header_dict = {}
            for bkey in header['_store'].keys():
                if bkey == 'py/object':
                    continue
                header_dict[header['_store'][bkey]['py/tuple'][0]] = header['_store'][bkey]['py/tuple'][1]
            return header_dict
        except KeyError:
            logger.exception('Got a key error maybe this json has been fixed')
            return header

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

            if self._fixheaders is True:
                cdf.set_headers(self._get_fixed_header(cdf.get_headers()))

            cdf_list.append(cdf)
        return cdf_list
