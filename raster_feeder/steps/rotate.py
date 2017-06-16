# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Stores latest steps in a rotating raster store group.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from os.path import join

import argparse
import contextlib
import logging
import sys

import netCDF4
# import numpy as np

from osgeo import osr
from osgeo import gdal

from raster_store import regions
from raster_store import datasets

from ..common import rotate
from . import config

logger = logging.getLogger(__name__)

"""
in rotate, create vrt and read it as array, then create region.
call rotate
done.
"""


@contextlib.contextmanager
def download(current=None):
    """ Dummy downloader for debugging purposes. """
    # return open file object
    path = 'steps_epf60_SydM-nc_20170519_1700.nc'
    logger.info('Using dummy file "{}".'.format(path))
    yield path


def extract_region(path):
    """
    Return latest steps data as raster store region.

    :param path: path to netCDF4 file.

    Due to the ease of the gdal warped vrt, gdal is used for the data part.
    However, due to the ease of use of the netCDF4 calendar functions, netCDF4
    is used for the time part.
    """
    with netCDF4.Dataset(path) as nc:
        # time
        valid_time = nc.variables['valid_time']
        time_units = valid_time.units
        time_data = valid_time[:]
        time = netCDF4.num2date(time_data, units=time_units)

        precipitation = nc.variables['precipitation']
        native_data = precipitation[0, :, ::-1]
        no_data_value = precipitation._FillValue

    native_projection = osr.GetUserInputAsWKT(str(config.NATIVE_PROJECTION))
    kwargs = {
        'geo_transform': config.NATIVE_GEO_TRANSFORM,
        'projection': native_projection,
        'no_data_value': no_data_value.item(),
    }
    with datasets.Dataset(native_data, **kwargs) as native_dataset:
        warped_projection = osr.GetUserInputAsWKT(str(config.WARPED_PROJECTION))
        warped = gdal.AutoCreateWarpedVRT(
            native_dataset,
            native_projection,
            warped_projection,
            gdal.GRA_NearestNeighbour,
            0.125,  # same as gdalwarp commandline utility
        )
        data = warped.ReadAsArray()

    return regions.Region.from_mem(
        time=time,
        bands=(0, config.DEPTH),
        fillvalue=no_data_value,
        projection=str(config.WARPED_PROJECTION),
        data=data,
        geo_transform=config.WARPED_GEO_TRANSFORM,
    )


def rotate_steps():
    """
    Rotate steps stores.
    """
    # retrieve updated data
    try:
        with download() as path:
            region = extract_region(path)
    except:
        logger.exception('Error:')
        return

    # rotate the stores
    name = config.STORE_NAME
    path = join(config.STORE_DIR, name)
    rotate(path=path, region=region, resource=name, label='rotate')


def get_parser():
    """ Return argument parser. """
    parser = argparse.ArgumentParser(
        description=__doc__
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
    )
    return parser


def main():
    """ Call command with args from parser. """
    # logging
    kwargs = vars(get_parser().parse_args())
    if kwargs.pop('verbose'):
        logging.basicConfig(**{
            'stream': sys.stderr,
            'level': logging.INFO,
        })
    else:
        logging.basicConfig(**{
            'level': logging.INFO,
            'format': '%(asctime)s %(levelname)s %(message)s',
            'filename': join(config.LOG_DIR, 'steps_rotate.log')
        })
    logging.basicConfig(**kwargs)

    # run
    rotate_steps(**kwargs)
