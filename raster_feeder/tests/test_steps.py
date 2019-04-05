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
from datetime import datetime
from mock import patch, MagicMock, DEFAULT
import osr
import io

from raster_feeder.tests.common import MockFTPServer
from raster_feeder.steps import config
from raster_feeder.steps.rotate import extract_region, rotate_steps
from raster_feeder.steps.init import init_steps
from raster_store.regions import Region
from raster_store import load, caches
from raster_store.stores import Store

TEST_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                              '../../var/data'))


@patch.multiple('raster_feeder.steps.rotate', FTPServer=DEFAULT, load=DEFAULT,
                rotate=DEFAULT, touch_lizard=DEFAULT, extract_region=DEFAULT)
class TestRotateSteps(unittest.TestCase):
    def setUp(self, **patches):
        self.mock_ftp = MockFTPServer(dict())
        self.mock_store = MagicMock(Store)
        self.empty_stream = io.BytesIO()

    def test_pick_correct(self, **patches):
        patches['FTPServer'].return_value = self.mock_ftp
        patches['load'].return_value = self.mock_store
        self.mock_store.period = None

        correct = 'IDR311EN.RF3.20180323100000.nc'
        self.mock_ftp.files = {correct: self.empty_stream,
                               'IDR311AR.201803241000.nc': None}
        rotate_steps()

        extract_region_patch = patches['extract_region']
        self.assertEquals(extract_region_patch.call_count, 1)
        self.assertIn(correct, extract_region_patch.call_args[0][0])

    def test_pick_newest(self, **patches):
        patches['FTPServer'].return_value = self.mock_ftp
        patches['load'].return_value = self.mock_store
        self.mock_store.period = None

        correct = 'IDR311EN.RF3.20180323100000.nc'
        self.mock_ftp.files = {'IDR311EN.RF3.20180322100000.nc': None,
                               correct: self.empty_stream}
        rotate_steps()

        extract_region_patch = patches['extract_region']
        self.assertEquals(extract_region_patch.call_count, 1)
        self.assertIn(correct, extract_region_patch.call_args[0][0])

    def test_no_files(self, **patches):
        patches['FTPServer'].return_value = self.mock_ftp
        patches['load'].return_value = self.mock_store
        self.mock_store.period = None

        self.mock_ftp.files = {}
        rotate_steps()

        extract_region_patch = patches['extract_region']
        self.assertEquals(extract_region_patch.call_count, 0)

    def test_file_already_done(self, **patches):
        patches['FTPServer'].return_value = self.mock_ftp
        patches['load'].return_value = self.mock_store
        self.mock_store.period = (datetime(2018, 3, 23, 10, 0),
                                  datetime(2018, 3, 25, 10, 0))

        correct = 'IDR311EN.RF3.20180323100000.nc'
        self.mock_ftp.files = {correct: self.empty_stream}
        rotate_steps()

        extract_region_patch = patches['extract_region']
        self.assertEquals(extract_region_patch.call_count, 0)

    def test_file_is_newer(self, **patches):
        patches['FTPServer'].return_value = self.mock_ftp
        patches['load'].return_value = self.mock_store
        self.mock_store.period = (datetime(2018, 3, 23, 10, 0),
                                  datetime(2018, 3, 25, 10, 0))

        correct = 'IDR311EN.RF3.20180323110000.nc'
        self.mock_ftp.files = {correct: self.empty_stream}
        rotate_steps()

        extract_region_patch = patches['extract_region']
        self.assertEquals(extract_region_patch.call_count, 1)
        self.assertIn(correct, extract_region_patch.call_args[0][0])

    def test_file_is_older(self, **patches):
        patches['FTPServer'].return_value = self.mock_ftp
        patches['load'].return_value = self.mock_store
        self.mock_store.period = (datetime(2018, 3, 23, 10, 0),
                                  datetime(2018, 3, 25, 10, 0))

        correct = 'IDR311EN.RF3.20180323090000.nc'
        self.mock_ftp.files = {correct: self.empty_stream}
        rotate_steps()

        extract_region_patch = patches['extract_region']
        self.assertEquals(extract_region_patch.call_count, 0)


class TestExtract(unittest.TestCase):
    def setUp(self):
        matched = [f for f in os.listdir(TEST_DATA_PATH)
                   if f.startswith('IDR311EN.RF3')]
        if len(matched) == 0:
            self.skipTest('NetCDF testfile for steps raindata not found.')
        else:
            self.path = os.path.join(TEST_DATA_PATH, matched[0])

    def test_bands(self):
        region = extract_region(self.path)
        self.assertEqual(region.box.data.shape[0], config.DEPTH)


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
        region = Region.from_mem(data=np.empty((config.DEPTH, 512, 512),
                                               dtype='f4'),
                                 time=[datetime.now()],
                                 bands=(0, config.DEPTH), fillvalue=0.,
                                 geo_transform=config.GEO_TRANSFORM,
                                 projection=proj)

        store = load(os.path.join(self.raster_path, 'steps/steps1'))
        store.update([region])

        expected_extent_native = (-256.0, 256.0, -256.0, 256.0)
        expected_extent_wgs84 = (
            148.047113, -35.536657, 153.700887, -30.951083,
        )
        self.assertEqual(store.geometry.GetEnvelope(), expected_extent_native)
        np.testing.assert_allclose(store.extent, expected_extent_wgs84)
