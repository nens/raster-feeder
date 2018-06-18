# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import os
import unittest
import io
import shutil
import tempfile
from datetime import datetime
from mock import patch, DEFAULT, MagicMock

import numpy as np
from numpy.testing import assert_allclose

import raster_feeder
from raster_feeder.alarmtester.config import NAME
from raster_feeder.alarmtester.init import init_alarmtester
from raster_feeder.alarmtester.rotate import rotate_alarmtester

from raster_store import caches
from raster_store import load

caches.cache.client = caches.DummyClient()


class TestRotateAlarmTester(unittest.TestCase):
    def setUp(self, **patches):
        self.store_path = tempfile.mkdtemp()
        self.now = datetime.utcnow()
        with patch("raster_feeder.alarmtester.config.STORE_DIR",
                   self.store_path) as _:
            init_alarmtester()

    def tearDown(self):
        if os.path.isdir(self.store_path):
            shutil.rmtree(self.store_path)

    @patch('raster_feeder.common.locker')
    def test_rotate(self, *patches):
        with patch("raster_feeder.alarmtester.config.STORE_DIR",
                   self.store_path) as _:
            rotate_alarmtester()
        store = load(os.path.join(self.store_path, NAME))
        assert store
