#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `cildata_util` package."""

import os
import tempfile
import logging
import shutil
import unittest
import configparser

from cildata_util import config
from cildata_util.config import CILDatabaseConfig
from cildata_util.config import CILDatabaseConfigMissingError
from cildata_util.config import CILDatabaseConfigParseError


class TestConfig(unittest.TestCase):
    """Tests for `cildatadownloader` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_setup_logging(self):
        """Test something."""
        logger = logging.getLogger('foo')
        config.setup_logging(logger)
        self.assertEqual(logger.getEffectiveLevel(), logging.WARNING)

        config.setup_logging(logger, loglevel='DEBUG')
        self.assertEqual(logger.getEffectiveLevel(), logging.DEBUG)

        config.setup_logging(logger, loglevel='INFO')
        self.assertEqual(logger.getEffectiveLevel(), logging.INFO)

        config.setup_logging(logger, loglevel='ERROR')
        self.assertEqual(logger.getEffectiveLevel(), logging.ERROR)

        config.setup_logging(logger, loglevel='CRITICAL')
        self.assertEqual(logger.getEffectiveLevel(), logging.CRITICAL)

    def test_cildatabase_config_none_passed_in(self):
        try:
            CILDatabaseConfig(None)
        except CILDatabaseConfigMissingError as e:
            self.assertTrue('None passed in for config file' in str(e))

    def test_cildatabase_config_no_file(self):
        temp_dir = tempfile.mkdtemp()
        try:
            nonexist = os.path.join(temp_dir, 'nonexistant')

            try:
                CILDatabaseConfig(nonexist)
            except CILDatabaseConfigMissingError as e:
                self.assertTrue('Config file not found on '
                                'filesystem' in str(e))
        finally:
            shutil.rmtree(temp_dir)

    def test_cildatabase_emptyconfig_test_get_param(self):
        temp_dir = tempfile.mkdtemp()
        try:
            emptyfile = os.path.join(temp_dir, 'empty')
            open(emptyfile, 'a').close()
            dbconfig = CILDatabaseConfig(emptyfile)
            try:
                dbconfig.get_user()
            except CILDatabaseConfigParseError as e:
                self.assertTrue('No postgres section found in' in str(e))
        finally:
            shutil.rmtree(temp_dir)

    def test_valid_config(self):
        temp_dir = tempfile.mkdtemp()
        try:
            con = configparser.ConfigParser()
            con.add_section(CILDatabaseConfig.POSTGRES_SECTION)
            con.set(CILDatabaseConfig.POSTGRES_SECTION,
                    CILDatabaseConfig.POSTGRES_USER, 'user')
            con.set(CILDatabaseConfig.POSTGRES_SECTION,
                    CILDatabaseConfig.POSTGRES_PASS, 'pass')
            con.set(CILDatabaseConfig.POSTGRES_SECTION,
                    CILDatabaseConfig.POSTGRES_HOST, 'host')

            con.set(CILDatabaseConfig.POSTGRES_SECTION,
                    CILDatabaseConfig.POSTGRES_DB, 'db')

            con.set(CILDatabaseConfig.POSTGRES_SECTION,
                    CILDatabaseConfig.POSTGRES_PORT, '5432')

            cfile = os.path.join(temp_dir, 'config')
            f = open(cfile, 'w')
            con.write(f)
            f.flush()
            f.close()

            dbconfig = CILDatabaseConfig(cfile)

            self.assertEqual(dbconfig.get_user(), 'user')
            self.assertEqual(dbconfig.get_password(), 'pass')
            self.assertEqual(dbconfig.get_host(), 'host')
            self.assertEqual(dbconfig.get_port(), 5432)
            self.assertEqual(dbconfig.get_database_name(), 'db')
        finally:
            shutil.rmtree(temp_dir)
