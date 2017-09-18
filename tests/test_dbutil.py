#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `cildata_util` package."""

import os
import tempfile
import shutil
import unittest

from cildata_util import dbutil
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

    def test_make_backup_of_json_with_nonexistant_file(self):
        temp_dir = tempfile.mkdtemp()
        try:
            # try where files does not exist
            not_exist = os.path.join(temp_dir, 'foo')
            dbutil.make_backup_of_json(not_exist)
        except IOError as e:
            self.assertTrue('No such file' in str(e))
        finally:
            shutil.rmtree(temp_dir)

    def test_make_backup_of_json(self):
        temp_dir = tempfile.mkdtemp()
        try:
            somefile = os.path.join(temp_dir, 'somefile.json')
            with open(somefile, 'w') as f:
                f.write('hi\n')
                f.flush()

            dbutil.make_backup_of_json(somefile)
            self.assertTrue(os.path.isfile(somefile))
            self.assertTrue(os.path.isfile(somefile + dbutil.BK_TXT + '0'))
            self.assertFalse(os.path.isfile(somefile + dbutil.BK_TXT + '1'))

            dbutil.make_backup_of_json(somefile)
            self.assertTrue(os.path.isfile(somefile))
            self.assertTrue(os.path.isfile(somefile + dbutil.BK_TXT + '0'))
            self.assertTrue(os.path.isfile(somefile + dbutil.BK_TXT + '1'))

            with open(somefile, 'w') as f:
                f.write('bye\n')
                f.flush()
            dbutil.make_backup_of_json(somefile)
            self.assertTrue(os.path.isfile(somefile))
            self.assertTrue(os.path.isfile(somefile + dbutil.BK_TXT + '0'))
            self.assertTrue(os.path.isfile(somefile + dbutil.BK_TXT + '1'))
            self.assertTrue(os.path.isfile(somefile + dbutil.BK_TXT + '2'))

            with open(somefile + dbutil.BK_TXT + '2', 'r') as f:
                self.assertEqual(f.read(), 'bye\n')

            with open(somefile + dbutil.BK_TXT + '2', 'r') as f:
                self.assertEqual(f.read(), 'bye\n')

            with open(somefile + dbutil.BK_TXT + '1', 'r') as f:
                self.assertEqual(f.read(), 'hi\n')

            with open(somefile + dbutil.BK_TXT + '0', 'r') as f:
                self.assertEqual(f.read(), 'hi\n')
        finally:
            shutil.rmtree(temp_dir)

    def test_md5(self):
        temp_dir = tempfile.mkdtemp()
        try:
            self.assertEqual(dbutil.md5(None), None)
            self.assertEqual(dbutil.md5(temp_dir), None)

            not_exist = os.path.join(temp_dir, 'foo')
            self.assertEqual(dbutil.md5(not_exist), None)

            emptyfile = os.path.join(temp_dir, 'empty')
            open(emptyfile, 'a').close()
            self.assertEqual(dbutil.md5(emptyfile),
                             'd41d8cd98f00b204e9800998ecf8427e')
            somefile = os.path.join(temp_dir, 'somefile.json')
            with open(somefile, 'w') as f:
                f.write('hi\n')
                f.flush()
            self.assertEqual(dbutil.md5(somefile),
                             '764efa883dda1e11db47671c4a3bbd9e')
        finally:
            shutil.rmtree(temp_dir)

    def test_download_file_num_retries_negative(self):
        self.assertEqual(dbutil.download_file('foo', '/tmp', numretries=-1),
                         (None, None, 999))

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
        self.assertEqual(cdf.get_has_raw(), None)

        cdf.set_checksum('123')
        cdf.set_download_success(True)
        cdf.set_download_time(0)
        cdf.set_file_name('somefile')
        cdf.set_is_video(False)
        cdf.set_mime_type('mimetype')
        cdf.set_headers(['hi'])
        cdf.set_file_size(123)
        cdf.set_localfile('local')
        cdf.set_has_raw(False)

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
        self.assertEqual(cdf.get_has_raw(), False)

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

            suffixoutfile = outfile + dbutil.JSON_SUFFIX
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

            suffixoutfile = outfile + dbutil.JSON_SUFFIX
            res_list = reader.get_cildatafiles(suffixoutfile)

            self.assertEqual(res_list[0].get_id(), cdf_list[0].get_id())
            self.assertEqual(res_list[0].get_file_name(),
                             cdf_list[0].get_file_name())
            self.assertEqual(res_list[1].get_id(), cdf_list[1].get_id())
            self.assertEqual(res_list[1].get_file_name(),
                             cdf_list[1].get_file_name())
        finally:
            shutil.rmtree(temp_dir)
