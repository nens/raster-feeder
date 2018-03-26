# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import os
import unittest

from raster_feeder.harmonie.rotate import extract_regions
from raster_feeder.harmonie import config


class TestExtract(unittest.TestCase):
    def setUp(self):
        self.path = os.path.abspath('../../var/harm36_v1_ned_surface_2018032606.tgz')
        if not os.path.exists(self.path):
            self.skipTest('KNMI testfile for harmonie data not found.')

    def test_extract_tgz(self):
        with open(self.path, 'rb') as fileobj:
            regions = extract_regions(fileobj)

        for params in config.PARAMETERS:
            self.assertIn(params['group'], regions)
