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


class TestDbutil(unittest.TestCase):
    """Tests for `cildatadownloader` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_cildatafile(self):
        cdf = CILDataFile(1)
        self.assertEqual(cdf.get_id(), 1)
        self.assertEqual(cdf.get_checksum(), None)
        self.assertEqual(cdf.get_download_success(), None)
        self.assertEqual(cdf.get_download_time(), None)
        self.assertEqual(cdf.get_file_name(), None)
        self.assertEqual(cdf.get_is_video(), None)
        self.assertEqual(cdf.get_mime_type(), None)

        cdf.set_checksum('123')
        cdf.set_download_success(True)
        cdf.set_download_time(0)
        cdf.set_file_name('somefile')
        cdf.set_is_video(False)
        cdf.set_mime_type('mimetype')

        self.assertEqual(cdf.get_id(), 1)
        self.assertEqual(cdf.get_checksum(), '123')
        self.assertEqual(cdf.get_download_success(), True)
        self.assertEqual(cdf.get_download_time(), 0)
        self.assertEqual(cdf.get_file_name(), 'somefile')
        self.assertEqual(cdf.get_is_video(), False)
        self.assertEqual(cdf.get_mime_type(), 'mimetype')





