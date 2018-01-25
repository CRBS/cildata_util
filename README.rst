===========================
CIL Data Extraction Utility
===========================



.. image:: https://pyup.io/repos/github/slash-segmentation/CIL_file_download_tool/shield.svg
     :target: https://pyup.io/repos/github/slash-segmentation/CIL_file_download_tool/
     :alt: Updates


Cell Image Library Data Extraction Utility is a Python application with a set 
of scripts to download & convert image/video data from the legacy Cell Image 
Library website. 
(https://cellimagelibrary.com)

For more information please visit our wiki page:

https://github.com/slash-segmentation/CIL_file_download_tool/wiki

Compatibility
-------------

 * Works with Python 2.6, 2.7

Dependencies
------------

 * `argparse <https://pypi.python.org/pypi/argparse>`_
 * `pg8000 <https://pypi.python.orig/pypi/pg8000>`_
 * `requests <https://pypi.python.org/pypi/requests>`_
 * `jsonpickle <https://pypi.python.org/pypi/jsonpickle>`_
 * `python-dateutil <https://pypi.python.org/pypi/python-dateutil>`_
 * `Pillow <https://pypi.python.org/pypi/Pillow>`_

Installation
------------

.. code:: bash

   # download this repo
   make dist
   pip install dist/cildata_util*whl

Usage
-----

Run 

.. code:: bash

   cildatadownloader.py --help

License
-------

See LICENSE.txt_

Bugs
----

Please report them `here <https://github.com/slash-segmentation/CIL_file_download_tool/issues>`_

Credits
---------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _LICENSE.txt: https://github.com/slash-segmentation/CIL_file_download_tool/blob/master/LICENSE.txt
.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage

