#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `cildata_util` package."""


import unittest

from cildata_util import cildatadownloader


class TestCildatadownloader(unittest.TestCase):
    """Tests for `cildatadownloader` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_parse_arguments(self):
        """Test something."""
        pargs = cildatadownloader._parse_arguments('hi', ['dbconf', 'somedir'])

        self.assertEqual(pargs.databaseconf, 'dbconf')
        self.assertEqual(pargs.destdir, 'somedir')
        self.assertEqual(pargs.loglevel, 'WARNING')

    def test_main(self):
        res = cildatadownloader.main(['yo', 'dbconf', 'somedir'])
        self.assertEqual(res, 0)
