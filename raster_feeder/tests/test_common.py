# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import os
import unittest

from raster_feeder.common import FTPServer
from raster_feeder.tests.common import TemporaryDirectory


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

    def test_get_match(self):
        with FTPServer(**self.login) as server:
            self.assertIn(self.name, server.listdir())

            match = server.get_latest_match(self.name)
            self.assertEqual(match, self.name)

            match = server.get_latest_match('nomatch')
            self.assertIsNone(match)

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
