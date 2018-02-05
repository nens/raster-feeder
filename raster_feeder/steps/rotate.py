# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Stores latest steps in a rotating raster store group.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from os.path import basename, join

import argparse
import contextlib
import logging
import sys

import netCDF4
import numpy as np
from osgeo import osr

from raster_store import regions

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
    path = 'IDR311EN.201802051210.nc'
    path = 'IDR311EN.201802051300.nc'
    path = 'IDR311EN.201802050920.nc'
    logger.info('Using dummy file "{}".'.format(path))
    yield path


def extract_region(path):
    """
    Return latest steps data as raster store region.

    :param path: path to netCDF4 file.

    Note that the region is not in the target store projection, but the raster
    store takes care of that.
    """
    with netCDF4.Dataset(path) as nc:
        # time
        valid_time = nc.variables['valid_time']
        time_units = valid_time.units
        time_data = valid_time[:]
        time = netCDF4.num2date(time_data, units=time_units)

        # select variable
        precipitation = nc.variables['precipitation']
        no_data_value = precipitation._FillValue.item()

        # read and put zeros at fillvalue
        values = precipitation[:]
        values[values == no_data_value] = 0
        values.shape = values.shape[0], values[0].size

        # ensemble member selection
        sums = values.sum(1)
        p75 = np.percentile(sums, 75)
        member = np.abs(sums - p75).argmin()
        logger.info('Member sums %s; selecting member %s.', sums, member)

        # extract member from original data with original fill values
        data = precipitation[member]

    # prepare meta messages
    template = 'Member {member} from {filename}.'
    filename = basename(path)
    meta = config.DEPTH * [template.format(member=member, filename=filename)]

    return regions.Region.from_mem(
        data=data,
        time=time,
        meta=meta,
        bands=(0, config.DEPTH),
        fillvalue=no_data_value,
        geo_transform=config.NATIVE_GEO_TRANSFORM,
        projection=osr.GetUserInputAsWKT(str(config.NATIVE_PROJECTION))
    )


def rotate_steps():
    """
    Rotate steps stores.
    """
    # retrieve updated data
    try:
        with download() as path:
            region = extract_region(path)
    except Exception:
        logger.exception('Error:')
        return

    # rotate the stores
    name = config.NAME
    path = join(config.STORE_DIR, name)
    rotate(path=path, region=region, resource=name)


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
