# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import os
import unittest
from mock import patch, Mock

from raster_feeder.steps.rotate import Server, extract_region


class TestServer(unittest.TestCase):
    def setUp(self):
        self.connection = Mock()
        self.listing = []
        self.connection.nlst.return_value = self.listing
        self.connection.retrbinary.return_value = None
        self.ftp = patch('ftplib.FTP', autospec=True,
                         return_value=self.connection)
        self.ftp.start()

    def tearDown(self):
        self.ftp.stop()

    def test_get_match(self):
        correct = 'IDR311EN.201803231000.nc'

        server = Server()
        self.assertIsNone(server.get_latest_match())

        self.listing.append(correct)
        self.assertEqual(server.get_latest_match(), correct)

        self.listing.append('IDR311AR.201803231000.nc')
        self.assertEqual(server.get_latest_match(), correct)

        self.listing.append('IDR311EN.201803221000.nc')
        self.assertEqual(server.get_latest_match(), correct)


class TestExtract(unittest.TestCase):
    def setUp(self):
        self.path =  os.path.abspath('../../var/IDR311EN.201803231000.nc')
        if not os.path.exists(self.path):
            self.skipTest('NetCDF testfile for steps raindata not found.')

    def test_smoke(self):
        extract_region(self.path)
