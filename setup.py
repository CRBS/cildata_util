#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

import os
import sys
import re

from setuptools import setup, find_packages

THIS_PACKAGE_NAME='cildata_util'

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

try:
    import rstcheck
    found_errors = False

    readme_errors = list(rstcheck.check(readme))
    if len(readme_errors) > 0:
        sys.stderr.write('\nErrors in README.rst [(line #, error)]\n' +
                         str(readme_errors) + '\n')
        found_errors = True

    history_errors = list(rstcheck.check(history))
    if len(history_errors) > 0:
        sys.stderr.write('\nErrors in HISTORY.rst [(line #, error)]\n' +
                         str(history_errors) + '\n')

        found_errors = True

    if 'sdist' in sys.argv or 'bdist_wheel' in sys.argv:
        if found_errors is True:
            sys.stderr.write('\n\nEXITING due to errors encountered in'
                             ' History.rst or Readme.rst.\n\nSee errors above\n\n')
            sys.exit(1)

except Exception as e:
    sys.stderr.write('WARNING: rstcheck library found, '
                     'unable to validate README.rst or HISTORY.rst\n')


requirements = [
    "argparse",
    "configparser",
    "pg8000",
    "requests",
    "jsonpickle",
    "python-dateutil",
    "Pillow"
]

setup_requirements = [
    # TODO(coleslaw481): put setup requirements (distutils extensions, etc.) here
]

test_requirements = [
    "argparse",
    "configparser",
    "pg8000",
    "requests",
    "jsonpickle",
    "python-dateutil",
    "Pillow"
]

# extract version from cildata_util/__init__.py
version='0.0.0'
with open(os.path.join(THIS_PACKAGE_NAME, '__init__.py')) as init_file:
    for line in init_file:
        if '__version__' in line:
            version_raw = re.sub("'", '', line.rstrip()).split('=')[1]
            version = re.sub(' ', '', version_raw)

setup(
    name='cildata_util',
    version=version,
    description="Contains utilities to extract files and data from legacy Cell Image Library",
    long_description=readme + '\n\n' + history,
    author="Chris Churas",
    author_email='churas.camera@gmail.com',
    url='https://github.com/CRBS/cildata_util',
    packages=find_packages(include=['cildata_util']),
    include_package_data=True,
    install_requires=requirements,
    zip_safe=False,
    keywords='cildata_util',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    scripts=['cildata_util/cildatadownloader.py',
             'cildata_util/cildatareport.py',
             'cildata_util/cildataconverter.py',
             'cildata_util/cildataupdatedb.py',
             'cildata_util/cildatathumbnailcreator.py'],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
)
