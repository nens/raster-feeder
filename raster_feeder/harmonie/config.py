# -*- coding: utf-8 -*<F7>-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
This is the HARMONIE configuration file. It allows for a 'HARMONIE'
localconfig, too, which should be put in the same directory as this module.
"""

# central config imports
from ..config import PACKAGE_DIR  # NOQA
from ..config import STORE_DIR  # NOQA
from ..config import LOG_DIR  # NOQA

# parameters, names, depths, see https://www.knmidata.nl/data-services/
# knmi-producten-overzicht/atmosfeer-modeldata/data-product-1

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

# those derived parameters need initialized stores, too:
DERIVED = (
    {'raster-store-group': 'harmonie-rad', 'steps': 48},
    {'raster-store-group': 'harmonie-evap', 'steps': 48},
    {'raster-store-group': 'harmonie-prcp', 'steps': 48},
)

# group from which to take the currently stored period
PERIOD_REFERENCE = 'harmonie-inr'  # because it actually starts at step 0

# geographical orientation
GEO_TRANSFORM = -0.0185, 0.037, 0, 55.8885, 0, -0.023
PROJECTION = 'EPSG:4326'

# dataplatform info
DATASET = {
    "dataset": "harmonie_arome_cy40_p1",
    "version": "0.2",
    "pattern": "harm40_v1_p1_%Y%m%d%H.tar",
    "step": {"hours": 6},
}

# -------------------------------------------
# settings to be overridden in localconfig.py
# -------------------------------------------

# FTP connection
FTP = dict(host='', user='', password='', path='')

# Lizard RasterStore UUIDs to touch
TOUCH_LIZARD = []

# KNMI dataplatform API
API_KEY = None

# import local settings
try:
    from .localconfig import *  # NOQA
except ImportError:
    pass
