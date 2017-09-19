#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `cildata_util` package."""

import os
import tempfile
import shutil
import unittest
import zipfile
from mock import Mock

from cildata_util import dbutil
from cildata_util.dbutil import CILDataFile
from cildata_util.dbutil import CILDataFileConverter


class FakeCILDataFile(object):
    """Fake object
    """
    pass


class TestCILDataFileConverter(unittest.TestCase):
    """Tests for `cildatadownloader` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_convert_cdf_is_none_or_download_failed(self):
        converter = CILDataFileConverter()
        self.assertEqual(converter.convert(None, None), None)
        cdf = CILDataFile(123)
        cdf.set_download_success(False)
        self.assertEqual(converter.convert(cdf, None), cdf)

        cdf.set_download_success(True)
        try:
            converter.convert(cdf, None)
        except ValueError as e:
            self.assertEqual(str(e), 'For id 123 file name is NOT set')

    def test_compare_extension_with_mimetype(self):
        converter = CILDataFileConverter()
        cdf = CILDataFile(123)
        cdf.set_file_name('foo.zip')
        converter._compare_extension_with_mimetype(cdf, '.zip')
        cdf.set_mime_type('application/zip')
        converter._compare_extension_with_mimetype(cdf, '.jpg')

    def test_extract_suffix_from_content_disposition(self):
        converter = CILDataFileConverter()
        try:
            converter._extract_suffix_from_content_disposition(None)
            self.fail('Expected ValueError')
        except ValueError as e:
            self.assertEqual(str(e), 'content_disp cannot be None')

        try:
            converter._extract_suffix_from_content_disposition('hellothere')
            self.fail('Expected ValueError')
        except ValueError as e:
            self.assertEqual(str(e), 'filename= not found in ' +
                             dbutil.CONTENT_DISPOSITION)

        res = converter._extract_suffix_from_content_disposition('attachment; '
                                                                 'filename=10 '
                                                                 'sec 2.avi')
        self.assertEqual(res, 'avi')

    def test_get_raw_video_extension(self):
        converter = CILDataFileConverter()
        cdf = CILDataFile(123)

        try:
            converter._get_raw_video_extension(cdf)
            self.fail('expected ValueError')
        except ValueError as e:
            self.assertEqual(str(e), 'No headers found for None')

        cdf.set_headers(dict())
        cdf.set_file_name(str(cdf.get_id()) + '.raw')
        try:
            converter._get_raw_video_extension(cdf)
            self.fail('expected ValueError')
        except ValueError as e:
            self.assertEqual(str(e), dbutil.CONTENT_DISPOSITION +
                             ' NOT in headers for 123.raw')

        headers = dict()
        headers[dbutil.CONTENT_DISPOSITION] = 'attachment; filename=10 2.avi'
        cdf.set_headers(headers)
        res = converter._get_raw_video_extension(cdf)
        self.assertEqual(res, '.avi')

    def test_change_suffix_on_cildatafile(self):
        temp_dir = tempfile.mkdtemp()
        try:
            converter = CILDataFileConverter()
            basefile = os.path.join(temp_dir, '123.raw')
            open(basefile, 'a').close()
            cdf = CILDataFile(123)
            cdf.set_file_name(str(cdf.get_id()) + dbutil.RAW_SUFFIX)
            converter._change_suffix_on_cildatafile(cdf, '.avi', temp_dir)

            self.assertEqual(os.path.isfile(basefile), False)

            newfile = os.path.join(temp_dir, '123.avi')
            self.assertEqual(os.path.isfile(newfile), True)

        finally:
            shutil.rmtree(temp_dir)

    def test_create_video_zip_file(self):
        temp_dir = tempfile.mkdtemp()
        try:
            myvid = os.path.join(temp_dir, '123.avi')
            with open(myvid, 'w') as f:
                f.write('hi')
                f.flush()
            cdf = CILDataFile(123)
            cdf.set_file_name('123.avi')

            converter = CILDataFileConverter()
            newcdf = converter._create_video_zip_file(cdf, temp_dir)
            self.assertEqual(newcdf.get_file_name(), str(cdf.get_id()) +
                             dbutil.ZIP_SUFFIX)
            self.assertEqual(newcdf.get_mime_type(), dbutil.ZIP_MIMETYPE)
            self.assertEqual(newcdf.get_file_size(), 142)
            self.assertTrue(newcdf.get_checksum(), 'hi')
            zfile = os.path.join(temp_dir, newcdf.get_file_name())
            self.assertTrue(os.path.isfile(zfile))
            zf = zipfile.ZipFile(zfile, mode='r', allowZip64=True)
            self.assertTrue(zf.infolist()[0].filename.endswith('123.avi'))
        finally:
            shutil.rmtree(temp_dir)

    def test_extract_image_from_zip_with_multiple_files_inside(self):
        temp_dir = tempfile.mkdtemp()
        try:
            myvid = os.path.join(temp_dir, '123.avi')
            with open(myvid, 'w') as f:
                f.write('hi')
                f.flush()

            myvid2 = os.path.join(temp_dir, '123.mpg')
            with open(myvid2, 'w') as f:
                f.write('bye')
                f.flush()

            cdf = CILDataFile(123)
            cdf.set_file_name(str(cdf.get_id()) + dbutil.ZIP_SUFFIX)

            z_file = os.path.join(temp_dir, cdf.get_file_name())
            zf = zipfile.ZipFile(z_file, mode='w')
            zf.write(myvid)
            zf.write(myvid2)
            zf.close()
            converter = CILDataFileConverter()
            try:
                converter._extract_image_from_zip(cdf, temp_dir)
                self.fail('Expected ValueError')
            except ValueError as e:
                self.assertTrue('Expected single file ' in str(e))

        finally:
            shutil.rmtree(temp_dir)

    def test_extract_image_from_zip_success(self):
        temp_dir = tempfile.mkdtemp()
        try:
            myvid = os.path.join(temp_dir, '123.avi')
            with open(myvid, 'w') as f:
                f.write('hi')
                f.flush()

            cdf = CILDataFile(123)
            cdf.set_file_name(str(cdf.get_id()) + dbutil.ZIP_SUFFIX)

            z_file = os.path.join(temp_dir, cdf.get_file_name())
            zf = zipfile.ZipFile(z_file, mode='w')
            zf.write(myvid)
            zf.close()
            converter = CILDataFileConverter()

            newcdf = converter._extract_image_from_zip(cdf, temp_dir)
            self.assertEqual(newcdf.get_file_name(), '123_orig.avi')
            self.assertEqual(newcdf.get_mime_type(), 'video/x-msvideo')
        finally:
            shutil.rmtree(temp_dir)

    def test_convert_video_non_raw(self):
        cdf = CILDataFile(123)
        cdf.set_file_name(str(cdf.get_id()) + dbutil.JPG_SUFFIX)
        converter = CILDataFileConverter()
        res = converter._convert_video(cdf, '/foo')
        self.assertEqual(res.get_id(), cdf.get_id())

    def test_convert_video(self):
        temp_dir = tempfile.mkdtemp()
        try:
            myvid = os.path.join(temp_dir, '123.raw')
            with open(myvid, 'w') as f:
                f.write('hi')
                f.flush()
            cdf = CILDataFile(123)
            cdf.set_file_name(str(cdf.get_id()) + dbutil.RAW_SUFFIX)
            headers = dict()
            headers[dbutil.CONTENT_DISPOSITION] = 'attachment; filename=39580.avi'
            cdf.set_headers(headers)
            converter = CILDataFileConverter()
            res = converter._convert_video(cdf, temp_dir)
            self.assertEqual(len(res), 2)
            self.assertEqual(res[0].get_file_name(), cdf.get_file_name())
            self.assertFalse(os.path.isfile(myvid))
            zfile = os.path.join(temp_dir, res[1].get_file_name())
            self.assertTrue(os.path.isfile(zfile))
        finally:
            shutil.rmtree(temp_dir)

    def test_convert_image_non_raw(self):
        cdf = CILDataFile(123)
        cdf.set_file_name(str(cdf.get_id()) + dbutil.JPG_SUFFIX)
        converter = CILDataFileConverter()
        res = converter._convert_image(cdf, '/foo')
        self.assertEqual(res.get_id(), cdf.get_id())

    def test_convert_image_not_zipfile(self):
        temp_dir = tempfile.mkdtemp()
        try:
            myvid = os.path.join(temp_dir, '123.raw')
            with open(myvid, 'w') as f:
                f.write('hi')
                f.flush()
            cdf = CILDataFile(123)
            cdf.set_file_name(str(cdf.get_id()) + dbutil.RAW_SUFFIX)
            converter = CILDataFileConverter()
            try:
                converter._convert_image(cdf, temp_dir)
                self.fail('Expected ValueError')
            except ValueError as e:
                self.assertTrue('NOT a zip file' in str(e))
        finally:
            shutil.rmtree(temp_dir)

    def test_convert_image(self):
        temp_dir = tempfile.mkdtemp()
        try:
            myvid = os.path.join(temp_dir, '123.gif')
            with open(myvid, 'w') as f:
                f.write('hi')
                f.flush()
            cdf = CILDataFile(123)
            cdf.set_file_name(str(cdf.get_id()) + dbutil.RAW_SUFFIX)
            z_file = os.path.join(temp_dir, cdf.get_file_name())
            zf = zipfile.ZipFile(z_file, mode='w')
            zf.write(myvid)
            zf.close()

            converter = CILDataFileConverter()
            res = converter._convert_image(cdf, temp_dir)
            self.assertEqual(len(res), 2)
            self.assertEqual(res[0].get_file_name(), cdf.get_file_name())
            self.assertTrue(os.path.isfile(myvid))
            zfile = os.path.join(temp_dir, res[1].get_file_name())
            self.assertTrue(os.path.isfile(zfile))
            orig_file = os.path.join(temp_dir, '123_orig.gif')
            self.assertTrue(os.path.isfile(orig_file))
        finally:
            shutil.rmtree(temp_dir)

    def test_convert(self):
        # TODO Complete testing this method
        self.assertEqual(1, 2)
