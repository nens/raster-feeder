# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import os
import shutil
import unittest
import tempfile
import numpy as np
import datetime
from mock import patch, Mock
import osr

from raster_feeder.steps import config
from raster_feeder.steps.rotate import Server, extract_region
from raster_feeder.steps.init import init_steps
from raster_store.regions import Region
from raster_store import load, caches


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
        self.path = 'IDR311EN.201803231010.nc'
        if not os.path.exists(self.path):
            self.skipTest('NetCDF testfile for steps raindata not found.')

    def test_smoke(self):
        extract_region(self.path)


class TestStore(unittest.TestCase):
    def setUp(self):
        self.raster_path = tempfile.mkdtemp()
        self.raster_path_patch = patch('raster_feeder.steps.config.STORE_DIR',
                                       new=self.raster_path)
        self.raster_path_patch.start()

        # replace with dummy cache
        caches.cache.client = caches.DummyClient()

    def tearDown(self):
        self.raster_path_patch.stop()
        if os.path.exists(self.raster_path):
            shutil.rmtree(self.raster_path)

    def test_extent(self):
        init_steps()

        proj = osr.GetUserInputAsWKT(str(config.PROJECTION))
        region = Region.from_mem(data=np.empty((config.DEPTH, 256, 256),
                                               dtype='f4'),
                                 time=[datetime.datetime.now()],
                                 bands=(0, config.DEPTH), fillvalue=0.,
                                 geo_transform=config.GEO_TRANSFORM,
                                 projection=proj)

        store = load(os.path.join(self.raster_path, 'steps/steps1'))
        store.update([region])

        expected_extent_native = (-256.0, 256.0, -256.0, 256.0)
        expected_extent_wgs84 = (148.01554961545378, -36.52832899894018,
                                 153.73245038454615, -31.936832031023386)

        self.assertEqual(store.geometry.GetEnvelope(), expected_extent_native)
        np.testing.assert_allclose(store.extent, expected_extent_wgs84)
