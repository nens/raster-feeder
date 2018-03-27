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
from mock import patch, Mock, MagicMock
import osr
from io import BytesIO

from raster_feeder.tests.common import MockFTPServer
from raster_feeder.steps import config
from raster_feeder.steps.rotate import extract_region, rotate_steps
from raster_feeder.steps.init import init_steps
from raster_store.regions import Region
from raster_store import load, caches
from raster_store.stores import Store


@patch('raster_feeder.steps.rotate.extract_region')
class TestRotateSteps(unittest.TestCase):
    def setUp(self):
        self.mock_ftp = MockFTPServer(dict())
        patch_ftp = patch('raster_feeder.steps.rotate.FTPServer',
                          return_value=self.mock_ftp)
        patch_ftp.start()
        self.addCleanup(patch_ftp.stop)

        self.mock_store = MagicMock(Store)
        self.empty_stream = BytesIO()
        patch_load_store = patch('raster_feeder.steps.rotate.load',
                                 return_value=self.mock_store)
        patch_load_store.start()
        self.addCleanup(patch_load_store.stop)

        patch_rotate = patch('raster_feeder.steps.rotate.rotate')
        patch_rotate.start()
        self.addCleanup(patch_rotate.stop)

        patch_touch = patch('raster_feeder.steps.rotate.touch_lizard')
        patch_touch.start()
        self.addCleanup(patch_touch.stop)

    def test_pick_EN_file(self, extract_region_patch):
        self.mock_store.period = None

        correct = 'IDR311EN.201803231000.nc'
        self.mock_ftp.files = {correct: self.empty_stream,
                               'IDR311AR.201803231000.nc': self.empty_stream}
        rotate_steps()

        self.assertEquals(extract_region_patch.call_count, 1)
        self.assertIn(correct, extract_region_patch.call_args[0][0])

    def test_pick_newest(self, extract_region_patch):
        self.mock_store.period = None

        correct = 'IDR311EN.201803231000.nc'
        self.mock_ftp.files = {'IDR311AR.201803221000.nc': self.empty_stream,
                               correct: self.empty_stream}
        rotate_steps()

        self.assertEquals(extract_region_patch.call_count, 1)
        self.assertIn(correct, extract_region_patch.call_args[0][0])

    def test_no_files(self, extract_region_patch):
        self.mock_store.period = None

        self.mock_ftp.files = {}
        rotate_steps()

        self.assertEquals(extract_region_patch.call_count, 0)

    def test_file_already_done(self, extract_region_patch):
        self.mock_store.period = (datetime.datetime(2018, 3, 23, 10, 0),
                                  datetime.datetime(2018, 3, 25, 10, 0))

        correct = 'IDR311EN.201803231000.nc'
        self.mock_ftp.files = {correct: self.empty_stream}
        rotate_steps()

        self.assertEquals(extract_region_patch.call_count, 0)

    def test_file_is_newer(self, extract_region_patch):
        self.mock_store.period = (datetime.datetime(2018, 3, 23, 10, 0),
                                  datetime.datetime(2018, 3, 25, 10, 0))

        correct = 'IDR311EN.201803231100.nc'
        self.mock_ftp.files = {correct: self.empty_stream}
        rotate_steps()

        self.assertEquals(extract_region_patch.call_count, 1)
        self.assertIn(correct, extract_region_patch.call_args[0][0])

    def test_file_is_older(self, extract_region_patch):
        self.mock_store.period = (datetime.datetime(2018, 3, 23, 10, 0),
                                  datetime.datetime(2018, 3, 25, 10, 0))

        correct = 'IDR311EN.201803230900.nc'
        self.mock_ftp.files = {correct: self.empty_stream}
        rotate_steps()

        self.assertEquals(extract_region_patch.call_count, 0)


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
