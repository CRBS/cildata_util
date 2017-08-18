#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    "argparse",
    "configparser",
    "pg8000",
    "requests"
]

setup_requirements = [
    # TODO(coleslaw481): put setup requirements (distutils extensions, etc.) here
]

test_requirements = [
    "argparse",
    "configparser",
    "pg8000",
    "requests"
]

setup(
    name='cildata_util',
    version='0.1.0',
    description="Contains utilities to extract files and data from legacy Cell Image Library",
    long_description=readme + '\n\n' + history,
    author="Chris Churas",
    author_email='churas.camera@gmail.com',
    url='https://github.com/slash-segmentation/CIL_file_download_tool',
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
    scripts = ['cildata_util/cildatadownloader.py'],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
)
