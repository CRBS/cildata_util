#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `cildata_util` package."""


import unittest

from cildata_util import cildatareport


class TestCildatareport(unittest.TestCase):
    """Tests for `cildatadownloader` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_parse_arguments(self):
        """Test something."""
        pargs = cildatareport._parse_arguments('hi', ['adir'])

        self.assertEqual(pargs.downloaddir, 'adir')
        self.assertEqual(pargs.loglevel, 'WARNING')

    def test_main_no_config(self):
        res = cildatareport.main(['yo', 'adir'])
        self.assertEqual(res, 0)
