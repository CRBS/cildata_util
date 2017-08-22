import os
import logging
import pg8000
import jsonpickle
import json


logger = logging.getLogger(__name__)


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
                           "is_video from cil_data_type where is_public=true" +
                           idfilter)

            for entry in cursor.fetchall():
                cdf = CILDataFile(entry[0])
                cdf.set_is_video(bool(entry[1]))
                cildatafiles.append(cdf)
        finally:
            cursor.close()
            self._conn.commit()

        return cildatafiles


class CILDataFileJsonPickleWriter(object):
    """Persists CILDataFile objects to a file using jsonpickle
    """
    SUFFIX = '.json'
    def __init__(self):
        """Constructor
        """
        pass

    def writeCILDataFileListToFile(self, outfile,
                                   cildatafile_list):

        """Writes CILDataFile objects in list to a file
           in json format
        """
        json_cdf_list = []
        for cdf in cildatafile_list:
            json_cdf_list.append(jsonpickle.encode(cdf))

        logger.debug('Writing out json file to ' + outfile)
        full_outfile = outfile + CILDataFileJsonPickleWriter.SUFFIX
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
        pass

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
