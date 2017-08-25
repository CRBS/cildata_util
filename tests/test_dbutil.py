#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `cildata_util` package."""

import os
import tempfile
import logging
import shutil
import unittest
import configparser
import time

from cildata_util.dbutil import Database
from cildata_util.dbutil import CILDataFile
from cildata_util.dbutil import CILDataFileJsonPickleWriter
from cildata_util.dbutil import CILDataFileListFromJsonPickleFactory

class FakeCILDataFile(object):
    """Fake object
    """
    pass

class TestDbutil(unittest.TestCase):
    """Tests for `cildatadownloader` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_database_get_alternate_connection(self):
        db = Database(None)
        db.set_alternate_connection('hi')
        self.assertEqual(db.get_connection(), 'hi')

    def test_database_with_none_for_config(self):
        db = Database(None)
        try:
            db.get_connection()
        except AttributeError as e:
            self.assertEqual(str(e),
                             "'NoneType' object has no attribute 'get_host'")

    def test_cildatafile(self):
        cdf = CILDataFile(1)
        self.assertEqual(cdf.get_id(), 1)
        self.assertEqual(cdf.get_checksum(), None)
        self.assertEqual(cdf.get_download_success(), None)
        self.assertEqual(cdf.get_download_time(), None)
        self.assertEqual(cdf.get_file_name(), None)
        self.assertEqual(cdf.get_is_video(), None)
        self.assertEqual(cdf.get_mime_type(), None)
        self.assertEqual(cdf.get_headers(), None)
        self.assertEqual(cdf.get_file_size(), None)
        self.assertEqual(cdf.get_localfile(), None)

        cdf.set_checksum('123')
        cdf.set_download_success(True)
        cdf.set_download_time(0)
        cdf.set_file_name('somefile')
        cdf.set_is_video(False)
        cdf.set_mime_type('mimetype')
        cdf.set_headers(['hi'])
        cdf.set_file_size(123)
        cdf.set_localfile('local')

        self.assertEqual(cdf.get_id(), 1)
        self.assertEqual(cdf.get_checksum(), '123')
        self.assertEqual(cdf.get_download_success(), True)
        self.assertEqual(cdf.get_download_time(), 0)
        self.assertEqual(cdf.get_file_name(), 'somefile')
        self.assertEqual(cdf.get_is_video(), False)
        self.assertEqual(cdf.get_mime_type(), 'mimetype')
        self.assertEqual(cdf.get_headers(), ['hi'])
        self.assertEqual(cdf.get_file_size(), 123)
        self.assertEqual(cdf.get_localfile(), 'local')

    def test_cildatafile_copy(self):
        fakecdf = FakeCILDataFile()
        cdf = CILDataFile(123)
        cdf.copy(fakecdf)
        self.assertEqual(cdf.get_id(), 123)
        self.assertEqual(cdf.get_checksum(), None)
        self.assertEqual(cdf.get_download_success(), None)
        self.assertEqual(cdf.get_download_time(), None)
        self.assertEqual(cdf.get_file_name(), None)
        self.assertEqual(cdf.get_is_video(), None)
        self.assertEqual(cdf.get_mime_type(), None)
        self.assertEqual(cdf.get_headers(), None)
        self.assertEqual(cdf.get_file_size(), None)

    def test_cildatafilepicklewriter_writeonecdf(self):
        cdf = CILDataFile(123)
        cdf.set_checksum('123')
        cdf.set_download_success(True)
        cdf.set_download_time(0)
        cdf.set_file_name('somefile')
        cdf.set_is_video(False)
        cdf.set_mime_type('mimetype')
        cdf.set_headers(['hi'])
        cdf.set_file_size(123)

        cdf_list = []
        cdf_list.append(cdf)
        temp_dir = tempfile.mkdtemp()
        try:
            writer = CILDataFileJsonPickleWriter()
            outfile = os.path.join(temp_dir, 'foo')
            writer.writeCILDataFileListToFile(outfile, cdf_list)
            reader = CILDataFileListFromJsonPickleFactory()

            suffixoutfile = outfile + CILDataFileJsonPickleWriter.SUFFIX
            res_list = reader.get_cildatafiles(suffixoutfile)

            self.assertEqual(res_list[0].get_id(), cdf_list[0].get_id())
            self.assertEqual(res_list[0].get_file_name(),
                             cdf_list[0].get_file_name())
        finally:
            shutil.rmtree(temp_dir)

    def test_cildatafilepicklewriter_writetwocdfs(self):
        cdf = CILDataFile(123)
        cdf.set_checksum('123')
        cdf.set_download_success(True)
        cdf.set_download_time(0)
        cdf.set_file_name('somefile')
        cdf.set_is_video(False)
        cdf.set_mime_type('mimetype')
        cdf.set_headers(['hi'])
        cdf.set_file_size(123)
        cdf_list = []
        cdf_list.append(cdf)

        cdf2 = CILDataFile(3465)
        cdf_list.append(cdf2)
        temp_dir = tempfile.mkdtemp()
        try:
            writer = CILDataFileJsonPickleWriter()
            outfile = os.path.join(temp_dir, 'foo')
            writer.writeCILDataFileListToFile(outfile, cdf_list)
            reader = CILDataFileListFromJsonPickleFactory()

            suffixoutfile = outfile + CILDataFileJsonPickleWriter.SUFFIX
            res_list = reader.get_cildatafiles(suffixoutfile)

            self.assertEqual(res_list[0].get_id(), cdf_list[0].get_id())
            self.assertEqual(res_list[0].get_file_name(),
                             cdf_list[0].get_file_name())
            self.assertEqual(res_list[1].get_id(), cdf_list[1].get_id())
            self.assertEqual(res_list[1].get_file_name(),
                             cdf_list[1].get_file_name())
        finally:
            shutil.rmtree(temp_dir)

"""
    def test_read_old_json_and_write_and_read(self):
        reader = CILDataFileListFromJsonPickleFactory()
        # cdf_list = reader.get_cildatafiles('/home/churas/src/CIL_file_download_tool/39602.json')
        cdf_list = reader.get_cildatafiles('/home/churas/tasks/cil_downloader/out/images/10001/10001.json')
        self.assertEqual(cdf_list[0].get_file_size(), None)
        self.assertEqual(str(cdf_list[0].get_headers()), "{u'Content-disposition': u'attachment; filename=10001.tif', u'Transfer-Encoding': u'chunked', u'Server': u'Jetty(6.1.18)', u'Connection': u'close', u'Date': u'Mon, 21 Aug 2017 22:20:40 GMT', u'Content-Type': u'image/tif'}")

        writer = CILDataFileJsonPickleWriter()
        writer.writeCILDataFileListToFile('/home/churas/yo', cdf_list)

        newcdf_list = reader.get_cildatafiles('/home/churas/yo.json')
        self.assertEqual(newcdf_list[0].get_file_size(), None)
        self.assertEqual(str(newcdf_list[0].get_headers()), "{u'Content-disposition': u'attachment; filename=10001.tif', u'Transfer-Encoding': u'chunked', u'Server': u'Jetty(6.1.18)', u'Connection': u'close', u'Date': u'Mon, 21 Aug 2017 22:20:40 GMT', u'Content-Type': u'image/tif'}")
"""

