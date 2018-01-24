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

    def test_parse_arguments(self):

        res = cildatathumbnailcreator._parse_arguments('hi', ['download',
                                                              'dest'])
        self.assertEqual(res.downloaddir, 'download')
        self.assertEqual(res.destdir, 'dest')
        self.assertEqual(res.loglevel, 'WARNING')
        self.assertEqual(res.sizes, '88,140,220,512')
        self.assertEqual(res.suffix, '.jpg')
        self.assertEqual(res.overwrite, False)

        res = cildatathumbnailcreator._parse_arguments('hi', ['download',
                                                              'dest',
                                                              '--log',
                                                              'DEBUG',
                                                              '--sizes',
                                                              '123',
                                                              '--suffix',
                                                              '.hi',
                                                              '--overwrite'])
        self.assertEqual(res.downloaddir, 'download')
        self.assertEqual(res.destdir, 'dest')
        self.assertEqual(res.loglevel, 'DEBUG')
        self.assertEqual(res.sizes, '123')
        self.assertEqual(res.suffix, '.hi')
        self.assertEqual(res.overwrite, True)

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

        res = cildatathumbnailcreator._extract_file_name_and_suffix('foojpg')
        self.assertEqual(res, ('foojpg', None))

        res = cildatathumbnailcreator._extract_file_name_and_suffix('foo.jpg')
        self.assertEqual(res, ('foo', '.jpg'))

        res = cildatathumbnailcreator._extract_file_name_and_suffix('/home/foo/')
        self.assertEqual(res, ('', None))

        res = cildatathumbnailcreator._extract_file_name_and_suffix('/home/foo/b.nana.jpg')
        self.assertEqual(res, ('b.nana', '.jpg'))

    def test_create_single_thumbnail(self):

        res = cildatathumbnailcreator._create_single_thumbnail(None, 50)
        self.assertEqual(res, None)

        res = cildatathumbnailcreator._create_single_thumbnail(None, -50)
        self.assertEqual(res, None)

        im = Image.new('RGB', (400, 500), color=(255, 0, 0))

        res = cildatathumbnailcreator._create_single_thumbnail(im, -50)
        self.assertEqual(res, None)

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
                _get_list_of_numeric_directories(None)
            self.assertEqual(res, [])

            res = cildatathumbnailcreator.\
                _get_list_of_numeric_directories(temp_dir)
            self.assertEqual(res, [])

            fakefile = os.path.join(temp_dir, '12345')
            open(fakefile, 'a').close()

            res = cildatathumbnailcreator. \
                _get_list_of_numeric_directories(fakefile)
            self.assertEqual(res, [])

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

    def test_create_thumbnail_images(self):
        temp_dir = tempfile.mkdtemp()
        try:
            im = Image.new('RGB', (400, 500), color=(255, 0, 0))
            img_file = os.path.join(temp_dir, '12345.jpg')
            im.save(img_file)

            img_file_nosuf = os.path.join(temp_dir, '5678jpg')
            im.save(img_file_nosuf, 'JPEG')

            im.close()

            res = cildatathumbnailcreator._create_thumbnail_images(img_file_nosuf,
                                                                   [88, 140],
                                                                   temp_dir)
            self.assertEqual(res, 2)

            badimg = os.path.join(temp_dir, '333.jpg')
            open(badimg, 'a').close()
            res = cildatathumbnailcreator._create_thumbnail_images(badimg,
                                                                   [88, 140],
                                                                   temp_dir)
            self.assertEqual(res, 1)

            res = cildatathumbnailcreator._create_thumbnail_images(img_file,
                                                               [-50],
                                                               temp_dir)
            self.assertEqual(res, 3)

            res = cildatathumbnailcreator._create_thumbnail_images(img_file,
                                                               [88, 140],
                                                               temp_dir)
            self.assertEqual(res, 0)

            eightyimg_file = os.path.join(temp_dir, '12345',
                                          '12345_thumbnailx88.jpg')
            self.assertTrue(os.path.isfile(eightyimg_file))
            im = Image.open(eightyimg_file)
            self.assertEqual(im.size, (88, 88))
            im.close()

            onefortyimg_file = os.path.join(temp_dir, '12345',
                                            '12345_thumbnailx140.jpg')
            self.assertTrue(os.path.isfile(onefortyimg_file))
            im = Image.open(onefortyimg_file)
            self.assertEqual(im.size, (140, 140))
            im.close()
        finally:
            shutil.rmtree(temp_dir)

    def test_create_thumbnails_for_entries_in_subdirs(self):
        temp_dir = tempfile.mkdtemp()
        try:
            suffix = cildatathumbnailcreator.DEFAULT_SUFFIX
            res = cildatathumbnailcreator.\
                _create_thumbnails_for_entries_in_subdirs(temp_dir,
                                                          [88],
                                                          temp_dir,
                                                          suffix)
            self.assertEqual(res, 0)

            dest_dir = os.path.join(temp_dir, 'foo')

            onedir = os.path.join(temp_dir,'123')
            os.makedirs(onedir, mode=0o755)

            res = cildatathumbnailcreator. \
                _create_thumbnails_for_entries_in_subdirs(temp_dir,
                                                          [88],
                                                          dest_dir,
                                                          suffix)

            self.assertEqual(res, 0)
            self.assertFalse(os.path.isdir(dest_dir))

            oneimg = os.path.join(onedir, '123.jpg')
            im = Image.new('RGB', (400, 500), color=(255, 0, 0))
            im.save(oneimg)
            im.close()


            res = cildatathumbnailcreator. \
                _create_thumbnails_for_entries_in_subdirs(temp_dir,
                                                          [88],
                                                          dest_dir,
                                                          suffix)

            self.assertEqual(res, 0)
            thumby_file = os.path.join(dest_dir, '123', '123_thumbnailx88.jpg')
            self.assertTrue(os.path.isfile(thumby_file))
            im = Image.open(thumby_file)
            self.assertEqual(im.size, (88, 88))
            im.close()

            twodir = os.path.join(temp_dir,'124')
            os.makedirs(twodir, mode=0o755)
            twoimg = os.path.join(twodir, '124.jpg')
            im = Image.new('RGB', (400, 500), color=(0, 255, 0))
            im.save(twoimg)
            im.close()

            shutil.rmtree(dest_dir)

            res = cildatathumbnailcreator. \
                _create_thumbnails_for_entries_in_subdirs(temp_dir,
                                                          [88],
                                                          dest_dir,
                                                          suffix)

            self.assertEqual(res, 0)
            thumby_file = os.path.join(dest_dir, '123', '123_thumbnailx88.jpg')
            self.assertTrue(os.path.isfile(thumby_file))
            im = Image.open(thumby_file)
            self.assertEqual(im.size, (88, 88))
            im.close()

            thumby_file = os.path.join(dest_dir, '124', '124_thumbnailx88.jpg')
            self.assertTrue(os.path.isfile(thumby_file))
            im = Image.open(thumby_file)
            self.assertEqual(im.size, (88, 88))
            im.close()
        finally:
            shutil.rmtree(temp_dir)

    def test_create_thumbnails(self):
        temp_dir = tempfile.mkdtemp()
        try:
            # test one image to convert
            dest_dir = os.path.join(temp_dir, 'foo')
            img_file = os.path.join(temp_dir, '123.jpg')
            im = Image.new('RGB', (400, 500), color=(255, 0, 0))
            im.save(img_file)
            im.close()
            p = cildatathumbnailcreator._parse_arguments('desc', [img_file,
                                                                  dest_dir])
            res = cildatathumbnailcreator._create_thumbnails(p)
            self.assertEqual(res, 0)
            thumby_file = os.path.join(dest_dir, '123', '123_thumbnailx88.jpg')
            self.assertTrue(os.path.isfile(thumby_file))

            images_dir = os.path.join(temp_dir, 'images')
            os.makedirs(images_dir, mode=0o755)

            subdir = os.path.join(images_dir, '444')
            os.makedirs(subdir, mode=0o755)
            subimg_file = os.path.join(subdir, '444.jpg')
            im = Image.new('RGB', (400, 500), color=(255, 0, 0))
            im.save(subimg_file)
            im.close()

            videos_dir = os.path.join(temp_dir, 'videos')
            os.makedirs(videos_dir, mode=0o755)
            shutil.rmtree(dest_dir)

            p = cildatathumbnailcreator._parse_arguments('desc', [temp_dir,
                                                                  dest_dir])
            res = cildatathumbnailcreator._create_thumbnails(p)
            self.assertEqual(res, 0)
            thumby_file = os.path.join(dest_dir, '444', '444_thumbnailx88.jpg')
            self.assertTrue(os.path.isfile(thumby_file))
        finally:
            shutil.rmtree(temp_dir)

    def test_main(self):
        temp_dir = tempfile.mkdtemp()
        try:
            dest_dir = os.path.join(temp_dir, 'foo')
            res = cildatathumbnailcreator.main(['hi.py', temp_dir, dest_dir])
            self.assertEqual(res, 0)

            # test with invalid path to get non zero exit
            res = cildatathumbnailcreator.main(['hi.py', dest_dir, temp_dir])
            self.assertEqual(res, 1)

        finally:
            shutil.rmtree(temp_dir)




