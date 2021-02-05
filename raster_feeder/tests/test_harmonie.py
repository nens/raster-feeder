# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
# -*- coding: utf-8 -*-

import os
import unittest
import io
from datetime import datetime
from unittest.mock import patch, DEFAULT, MagicMock

import numpy as np
from numpy.testing import assert_allclose

from raster_feeder.tests.common import MockFTPServer
from raster_feeder.harmonie.rotate import extract_regions, rotate_harmonie
from raster_feeder.harmonie.rotate import vapor_pressure_slope, makkink
from raster_feeder.harmonie import config
from raster_store.stores import Store


class TestExtract(unittest.TestCase):
    def setUp(self):
        self.path = os.path.abspath('../../var/'
                                    'harm40_v1_p1_2018032606.tar')
        if not os.path.exists(self.path):
            self.skipTest('Testfile for KNMI harmonie data not found.')

    def test_extract_tgz(self):
        with open(self.path, 'rb') as fileobj:
            regions = extract_regions(fileobj)

        for params in config.PARAMETERS:
            self.assertIn(params['group'], regions)
            region = regions[params['group']]

            # check the time values
            self.assertEqual(region.box.bands, (0, params['steps']))
            self.assertEqual(len(region.time), params['steps'])
            if params['steps'] == 49:
                firsttime = datetime(2018, 3, 26, 6, 0)
            elif params['steps'] == 48:
                firsttime = datetime(2018, 3, 26, 7, 0)
            self.assertEqual(region.time[0], firsttime)

            # check the projection
            self.assertEqual(region.box.projection, config.PROJECTION)
            expected_extent = (-0.0185, 48.9885, 11.0815, 55.8885)
            self.assertAlmostEquals(region.border.extent, expected_extent)

            # check the actual data
            self.assertEqual(region.box.data.shape[0], params['steps'])
            self.assertEqual(region.box.data.shape[1:], (300, 300))

        # test the relation between prcp and cr
        assert_allclose(np.cumsum(regions['harmonie-prcp'].box.data, 0),
                        regions['harmonie-cr'].box.data, rtol=0.001)
        # test the relation between rad and crad
        assert_allclose(np.cumsum(regions['harmonie-rad'].box.data, 0) * 3600,
                        regions['harmonie-crad'].box.data, rtol=0.001)


class TestMakkink(unittest.TestCase):
    def test_vapor_pressure_slope(self):
        # test values from wiki table (in mbar, we do kPa)
        # https://nl.wikipedia.org/wiki/Referentie-gewasverdamping
        temperature = np.array([-5, 0, 10, 20, 30, 40])
        expected = np.array([0.32, 0.45, 0.83, 1.45, 2.44, 3.94]) / 10.
        actual = vapor_pressure_slope(temperature)
        assert_allclose(actual, expected, atol=0.005)

    def test_makkink(self):
        N = 100
        radiation = np.random.random(N) * 1000  # range 0 - 1000 W / m2
        temperature = np.random.random(N) * 30  # range 0 - 30 degC

        s = vapor_pressure_slope(temperature)
        expected = 0.65 * s / (s + 0.066) * radiation
        expected /= 2.45e6 * 1e3   # [m / s]
        expected *= 1000. * 3600.  # [mm / d]

        actual = makkink(radiation, temperature)

        assert_allclose(actual, expected)

        # in the order of 1-10 mm / day
        assert_allclose(actual.mean(), 0.2, atol=0.2)
