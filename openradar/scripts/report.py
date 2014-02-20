#!/usr/bin/python
# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Created on Wed Feb 12 16:10:32 2014

Tom van Steijn, RHDHV
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import glob
import logging
import os
import sys

import pandas as pd

from openradar import arguments
from openradar import config
from openradar import scans


def expected_files(radarcode, datetimes):
    """ Generate list of expected files based on radar code and datetimes. """
    # generate expected path from scan signature
    files = []
    for scandatetime in datetimes:
        scan = scans.ScanSignature(scancode=radarcode
        , scandatetime=scandatetime)
        files.append(scan.get_scanpath())
    return files


def available_files(radar_dir, start):
    """ Generate list of available files for given date. """
    # Get radar_dir names from start datetime
    year = str(start.year)
    month = str(start.month).zfill(2)
    day = str(start.day).zfill(2)

    # List all content in specified radar_dir
    files = glob.glob(os.path.join(radar_dir, '*', year, month, day, '*'))
    return files


def check_availability(radar_dir, radars, (start, end)):
    """ Check data availability for given timestep. """
    # Generate expected date range
    end = end - pd.DateOffset(minutes=5)
    datetimes = pd.date_range(start=start, end=end, freq='5Min')

    # Generate expected files
    expected = expected_files(radars, datetimes)

    # Locate available files
    found = available_files(radar_dir, start)

    # Set difference and fraction    
    missing = set(expected) - set(found)
    frac = 1 - len(missing) / len(expected)
    return frac, missing


def period_availability(radar_dir, radars, (start, end), identities):
    """ Calculate data availability per radar for defined period. """
    # Generate date range within period in daily timestep
    period = pd.date_range(start=start, end=end, freq='D')

    # Get availability of files per day, for all radars
    availability = {}
    offset = pd.DateOffset(days=1)
    for radar in radars:
        radar_availability = {}
        for start, end in zip(period, period + offset):
            frac, missing = check_availability(radar_dir,
                radar, (start, end))
            radar_availability[start] = frac
        availability[radar] = radar_availability

    # To pandas timeseries
    availability = pd.DataFrame(availability)

    # Maximum for identities in radar codes (e.g. 'ase' and 'ess' )
    for primary, secondary in identities:
        availability[primary] = availability[[primary, secondary]].sum(axis=1)
        availability = availability.drop(secondary, axis=1)
    
    # Mean for all radars
    availability['radars'] = availability.mean(axis=1)

    return availability, missing


def export_csv(report, report_dir, report_type):
    """ Export report as comma separated values. """
    # Get start and end datetime as string
    start = report.index[0].strftime('%Y%m%d%H%M%S')
    end = report.index[-1].strftime('%Y%m%d%H%M%S')

    # Build filepath
    csvpath = '_'.join([report_type, start, end]) + '.csv'
    csvpath = os.path.join(report_dir, csvpath)

    # Export to csv
    report.to_csv(csvpath, na_rep=-999, float_format='%.3f')


def availability_volumetric():
    """ Availability report for volumetric data. """

    # Get data dir
    radar_dir = config.RADAR_DIR    

    # Get radar codes (e.g. NL60)
    radars = config.ALL_RADARS

    # Get report dir
    report_dir = config.REPORT_DIR
    
    # Set identities in radar codes 
    identities = [('ess', 'ase')]
    
    # Availability as timeseries in daily timestep
    availability, missing = period_availability(radar_dir,
    radars, ('2014/1/1', '2014/1/31'), identities)
    export_csv(availability, report_dir, 'availability_volumetric')


def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    argument = arguments.Argument()
    parser = argument.parser(['range'])
    files.organize_from_path(**vars(parser.parse_args()))
    

if __name__ == '__main__':
    availability_volumetric()
