# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import os
import unittest
import tempfile
import shutil

from raster_feeder.common import FTPServer


class TemporaryDirectory(object):
    """
    Context manager for tempfile.mkdtemp().
    This class is available in python +v3.2.
    """
    def __enter__(self):
        self.dir_name = tempfile.mkdtemp()
        return self.dir_name

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self.dir_name)


class TestFTPServer(unittest.TestCase):
    def setUp(self):
        self.login = dict(host='speedtest.tele2.net', user='anonymous',
                          password='')
        self.name = '1KB.zip'

    def test_connect(self):
        server = FTPServer(**self.login)
        server.close()

    def test_context_manager(self):
        with FTPServer(**self.login) as _:
            pass

    def test_list(self):
        with FTPServer(**self.login) as server:
            self.assertIn(self.name, server.listdir())

    def test_download_to_stream(self):
        with FTPServer(**self.login) as server:
            stream = server.retrieve_to_stream(self.name)

        self.assertEqual(len(stream.read()), 1024)

    def test_download_to_path(self):
        with TemporaryDirectory() as path:
            filepath = os.path.join(path, self.name)
            with FTPServer(**self.login) as server:
                server.retrieve_to_path(self.name, filepath)

            with open(filepath, 'rb') as stream:
                self.assertEqual(len(stream.read()), 1024)
