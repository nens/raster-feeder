#!/usr/bin/
# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import unittest
import datetime

from openradar import utils


class TestUtils(unittest.TestCase):
    """ Testing functions """

    def setUp(self):
        pass

    def test_get_groundfile_datetimes(self):
        # Five minutes
        dt_calculation = datetime.datetime(2013, 2, 3, 4, 5)
        results = list(utils.get_groundfile_datetimes(
            date=dt_calculation, timeframe='f', prodcode='r',
        ))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], datetime.datetime(2013, 2, 3, 4, 6))
        self.assertEqual(results[1], datetime.datetime(2013, 2, 3, 4, 5))

        results = list(utils.get_groundfile_datetimes(
            date=dt_calculation, timeframe='f', prodcode='n',
        ))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], datetime.datetime(2013, 2, 3, 5, 6))
        self.assertEqual(results[1], datetime.datetime(2013, 2, 3, 5, 5))

        results = list(utils.get_groundfile_datetimes(
            date=dt_calculation, timeframe='f', prodcode='a',
        ))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], datetime.datetime(2013, 2, 5, 4, 6))
        self.assertEqual(results[1], datetime.datetime(2013, 2, 5, 4, 5))

        # Hours
        dt_calculation = datetime.datetime(2013, 2, 3, 4, 0)
        results = list(utils.get_groundfile_datetimes(
            date=dt_calculation, timeframe='h', prodcode='r',
        ))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], datetime.datetime(2013, 2, 3, 4, 0))

        results = list(utils.get_groundfile_datetimes(
            date=dt_calculation, timeframe='h', prodcode='n',
        ))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], datetime.datetime(2013, 2, 3, 5, 0))

        results = list(utils.get_groundfile_datetimes(
            date=dt_calculation, timeframe='h', prodcode='a',
        ))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], datetime.datetime(2013, 2, 5, 4, 0))

        # Days
        dt_calculation = datetime.datetime(2013, 2, 3, 8)
        results = list(utils.get_groundfile_datetimes(
            date=dt_calculation, timeframe='d', prodcode='r',
        ))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], datetime.datetime(2013, 2, 3, 8))

        results = list(utils.get_groundfile_datetimes(
            date=dt_calculation, timeframe='d', prodcode='n',
        ))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], datetime.datetime(2013, 2, 3, 9))
        self.assertEqual(results[1], datetime.datetime(2013, 2, 3, 8))

        results = list(utils.get_groundfile_datetimes(
            date=dt_calculation, timeframe='d', prodcode='a',
        ))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], datetime.datetime(2013, 2, 5, 8))
