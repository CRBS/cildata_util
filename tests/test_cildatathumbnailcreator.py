#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `cildata_util` package."""

import os
import tempfile
import shutil
import unittest

from PIL import Image
from cildata_util import cildatathumbnailcreator


class TestCILDataThumbnailCreator(unittest.TestCase):
    """Tests for `cildatathumbnailcreator` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_get_size_list_from_arg(self):
        res = cildatathumbnailcreator._get_size_list_from_arg(None)
        self.assertEqual(res, [])

        res = cildatathumbnailcreator._get_size_list_from_arg('')
        self.assertEqual(res, [])

        res = cildatathumbnailcreator._get_size_list_from_arg('0')
        self.assertEqual(res, [])

        res = cildatathumbnailcreator._get_size_list_from_arg('3')
        self.assertEqual(res, [3])

        res = cildatathumbnailcreator._get_size_list_from_arg('4.5')
        self.assertEqual(res, [])

        res = cildatathumbnailcreator._get_size_list_from_arg('88,220, 40, 90')
        self.assertEqual(res, [88, 220, 40, 90])

    def test__extract_file_name_and_suffix(self):
        res = cildatathumbnailcreator._extract_file_name_and_suffix(None)
        self.assertEqual(res, (None, None))

        res = cildatathumbnailcreator._extract_file_name_and_suffix('')
        self.assertEqual(res, ('', None))

        res = cildatathumbnailcreator._extract_file_name_and_suffix('foo.jpg')
        self.assertEqual(res, ('foo', '.jpg'))

        res = cildatathumbnailcreator._extract_file_name_and_suffix('/home/foo/')
        self.assertEqual(res, ('', None))

        res = cildatathumbnailcreator._extract_file_name_and_suffix('/home/foo/b.nana.jpg')
        self.assertEqual(res, ('b.nana', '.jpg'))

    def test_create_single_thumbnail(self):
        im = Image.new('RGB', (400, 500), color=(255, 0, 0))

        res = cildatathumbnailcreator._create_single_thumbnail(im, 50)
        self.assertEqual(res.size, (50, 50))
        self.assertEqual(res.getpixel((0, 0)), (0, 0, 0))
        self.assertEqual(res.getpixel((25, 0)), (255, 0, 0))

        res.close()

        im = Image.new('RGB', (500, 400), color=(255, 0, 0))
        res = cildatathumbnailcreator._create_single_thumbnail(im, 41)
        self.assertEqual(res.size, (41, 41))
        self.assertEqual(res.getpixel((0, 0)), (0, 0, 0))
        self.assertEqual(res.getpixel((0, 3)), (0, 0, 0))
        self.assertEqual(res.getpixel((0, 4)), (255, 0, 0))
        self.assertEqual(res.getpixel((0, 37)), (0, 0, 0))
        self.assertEqual(res.getpixel((0, 35)), (255, 0, 0))
        self.assertEqual(res.getpixel((0, 25)), (255, 0, 0))
        res.close()

    def test_get_list_of_numeric_directories(self):
        temp_dir = tempfile.mkdtemp()
        try:
            res = cildatathumbnailcreator.\
                _get_list_of_numeric_directories(temp_dir)
            self.assertEqual(res, [])

            fakefile = os.path.join(temp_dir, '12345')
            open(fakefile, 'a').close()
            res = cildatathumbnailcreator. \
                _get_list_of_numeric_directories(temp_dir)
            self.assertEqual(res, [])

            fakedir = os.path.join(temp_dir, '4fb4')
            os.makedirs(fakedir, mode=0o755)
            res = cildatathumbnailcreator. \
                _get_list_of_numeric_directories(temp_dir)
            self.assertEqual(res, [])

            dirone = os.path.join(temp_dir, '12')
            os.makedirs(dirone, mode=0o755)
            res = cildatathumbnailcreator. \
                _get_list_of_numeric_directories(temp_dir)
            self.assertEqual(res, ['12'])

            dirtwo = os.path.join(temp_dir, '45986')
            os.makedirs(dirtwo, mode=0o755)
            res = cildatathumbnailcreator. \
                _get_list_of_numeric_directories(temp_dir)
            self.assertEqual(len(res), 2)
            self.assertTrue('12' in res)
            self.assertTrue('45986' in res)

        finally:
            shutil.rmtree(temp_dir)

