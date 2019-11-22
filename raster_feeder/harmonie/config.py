# -*- coding: utf-8 -*<F7>-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
This is the HARMONIE configuration file. It allows for a 'HARMONIE'
localconfig, too, which should be put in the same directory as this module.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from os.path import join

# central config imports
from ..config import BUILDOUT_DIR  # NOQA
from ..config import LOG_DIR       # NOQA

# parameters, names, depths, see http://projects.knmi.nl/datacentrum/
# catalogus/catalogus/content/nl-nwp-harm40-grid-p1.html

# rain intensity
rain_intensity = {
    'raster-store-group': 'harmonie-inr',
    'indicatorOfParameter': 181,
    'typeOfLevel': 'heightAboveGround',
    'level': 0,
    'timeRangeIndicator': 0,
    'steps': 49,  # available at first step (0 - 48 hr)
}

# rain cumulative sum
rain_cumulative_sum = {
    'raster-store-group': 'harmonie-cr',
    'indicatorOfParameter': 181,
    'typeOfLevel': 'heightAboveGround',
    'level': 0,
    'timeRangeIndicator': 4,
    'steps': 48,  # starts at second step (1 - 48 hr)
}

# air temperature at 2 m in Kelvin
air_temperature = {
    'raster-store-group': 'harmonie-temp',
    'indicatorOfParameter': 11,
    'typeOfLevel': 'heightAboveGround',
    'level': 2,
    'timeRangeIndicator': 0,
    'steps': 49,  # available at first step (0 - 48 hr)
}

# cumulative global radiation in J / m2
cumulative_radiation = {
    'raster-store-group': 'harmonie-crad',
    'indicatorOfParameter': 117,
    'typeOfLevel': 'heightAboveGround',
    'level': 0,
    'timeRangeIndicator': 4,
    'steps': 48,  # starts at second step (1 - 48 hr)
}

# note that rotate.extract() may derives additional parameters from these
# datasets
PARAMETERS = (
    rain_intensity,
    rain_cumulative_sum,
    air_temperature,
    cumulative_radiation,
)

# group from which to take the currently stored period
PERIOD_REFERENCE = 'harmonie-inr'  # because it actually starts at step 0

# geographical orientation
GEO_TRANSFORM = -0.0185, 0.037, 0, 55.8885, 0, -0.023
PROJECTION = 'EPSG:4326'

# remote file name strftime() format
harm40_v1_p1_2019112018.tar
FORMAT = 'harm40_v1_p1_%Y%m%d%H.tar'
PATTERN = r'harm40_v1_p1_[0-9]{10}\.tar'

# -------------------------------------------
# settings to be overridden in localconfig.py
# -------------------------------------------

# FTP connection
FTP = dict(host='', user='', password='', path='')

# raster store location
STORE_DIR = join(BUILDOUT_DIR, 'var', 'store')

# Lizard RasterStore UUIDs to touch
TOUCH_LIZARD = []

# import local settings
try:
    from .localconfig import *  # NOQA
except ImportError:
    pass
