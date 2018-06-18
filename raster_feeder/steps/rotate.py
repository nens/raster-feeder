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
import json
import logging
import shutil
import sys
import tempfile
import math

import netCDF4
import numpy as np
from osgeo import osr, ogr, gdal

from raster_store import load
from raster_store import regions

from ..common import rotate, touch_lizard, FTPServer
from . import config

logger = logging.getLogger(__name__)

EPSG32756 = osr.SpatialReference()
EPSG32756.ImportFromEPSG(32756)


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
        prcp = variable[:]  # shape: (10, 74, 512, 512)
        fillvalue = variable._FillValue.item()

    # transform the region of interest to indices in the array

    # create the geometry
    x1, y1, x2, y2 = config.ROI_ESPG32756
    ring = ogr.Geometry(ogr.wkbLinearRing)
    poly = ogr.Geometry(ogr.wkbPolygon)
    ring.AddPoint(x1, y1)
    ring.AddPoint(x2, y1)
    ring.AddPoint(x2, y2)
    ring.AddPoint(x1, y2)
    ring.AddPoint(x1, y1)
    poly.AddGeometry(ring)
    poly.AssignSpatialReference(EPSG32756)

    # transform the geometry to native projection
    target = osr.SpatialReference()
    target.ImportFromProj4(str(config.PROJECTION))
    poly.TransformTo(target)
    x1_proj, x2_proj, y1_proj, y2_proj = poly.GetEnvelope()

    # transform the envelope to indices
    inv_geo_transform = gdal.InvGeoTransform(config.GEO_TRANSFORM)
    x1_px, y1_px = gdal.ApplyGeoTransform(inv_geo_transform, x1_proj, y1_proj)
    x2_px, y2_px = gdal.ApplyGeoTransform(inv_geo_transform, x2_proj, y2_proj)

    # swap the indices if necessary
    if x1_px > x2_px:
        x1_px, x2_px = x2_px, x1_px
    if y1_px > y2_px:
        y1_px, y2_px = y2_px, y1_px

    # don't set the step to -1 as we are only using the ROI for statistics
    x_slice = slice(int(x1_px), int(math.ceil(x2_px)) + 1)
    y_slice = slice(int(y1_px), int(math.ceil(y2_px)) + 1)

    # slice the region of interest from the array
    logger.info('Taking slice (y %d%d, x %d:%d) for member selection',
                y_slice.start, y_slice.stop, x_slice.start, x_slice.stop)
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
