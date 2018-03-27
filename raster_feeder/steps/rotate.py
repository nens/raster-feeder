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
import ftplib
import json
import logging
import shutil
import sys
import tempfile

import netCDF4
import numpy as np
from osgeo import osr

from raster_store import load
from raster_store import regions

from ..common import rotate, touch_lizard, FTPServer
from . import config

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def mkdtemp(*args, **kwargs):
    """ Self-cleaning tempdir. """
    dtemp = tempfile.mkdtemp(*args, **kwargs)
    try:
        yield dtemp
    finally:
        shutil.rmtree(dtemp)


def extract_region(path):
    """
    Return latest steps data as raster store region.

    :param path: path to netCDF4 file.

    Note that the region is not in the target store projection, but the raster
    store takes care of that.
    """
    with netCDF4.Dataset(path) as nc:
        # read timesteps
        variable = nc.variables['valid_time']
        units = variable.units
        time = netCDF4.num2date(variable[:], units=units).tolist()

        # read precipitation
        variable = nc.variables['precipitation']
        prcp = variable[:]
        fillvalue = variable._FillValue.item()

    # copy out a region of interest for member selection
    y_slice, x_slice = config.STATISTICS_ROI
    prcp_roi = prcp[:, :, y_slice, x_slice].copy()

    # replace fillvalues with zeros for member selection
    prcp_roi[prcp_roi == fillvalue] = 0

    # ensemble member selection
    sums = prcp_roi.reshape(len(prcp_roi), -1).sum(1)
    p75 = np.percentile(sums, 75)
    member = np.abs(sums - p75).argmin()
    logger.info('Member sums are %s.', sums)
    logger.info('Selecting member %s.', member)

    # select member
    data = prcp[member]

    # prepare meta messages
    metadata = json.dumps({'file': basename(path), 'member': member})
    meta = config.DEPTH * [metadata]

    return regions.Region.from_mem(
        data=data,
        time=time,
        meta=meta,
        bands=(0, config.DEPTH),
        fillvalue=fillvalue,
        geo_transform=config.GEO_TRANSFORM,
        projection=osr.GetUserInputAsWKT(str(config.PROJECTION))
    )


def rotate_steps():
    """
    Rotate steps stores.
    """
    # determine current store period
    period = load(join(config.STORE_DIR, config.NAME)).period
    if period is None:
        current = None
    else:
        current = period[0]

    # retrieve updated data
    server = FTPServer(**config.FTP)
    latest = server.get_latest_match(config.PATTERN)
    if latest is None:
        logger.info('No source files found on server, exiting.')
        return

    if current and latest <= current.strftime(config.FORMAT):
        logger.info('No update available, exiting.')
        return

    # download and process the file
    try:
        with mkdtemp() as tdir:
            path = join(tdir, latest)
            server.retrieve_to_path(name=latest, path=path)
            region = extract_region(path)
    except Exception:
        logger.exception('Error:')
        return

    # rotate the stores
    name = config.NAME
    path = join(config.STORE_DIR, name)
    rotate(path=path, region=region, resource=name)

    # touch lizard
    for raster_uuid in config.TOUCH_LIZARD:
        touch_lizard(raster_uuid)


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
