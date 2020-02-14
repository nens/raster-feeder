# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Stores latest steps in a rotating raster store group.
"""

import argparse
import contextlib
import json
import logging
import os
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
    with netCDF4.Dataset(path, 'r') as nc:
        # read timesteps
        variable = nc.variables['valid_time']
        units = variable.units
        time = netCDF4.num2date(variable[:], units=units).tolist()

        # read precipitation
        variable = nc.variables['precipitation']

        # NetCDF performs linear scaling and masking automatically
        nc.set_auto_maskandscale(True)
        prcp = variable[:].astype('f4')
        fillvalue = np.finfo(prcp.dtype).max

        # NetCDF will produce a MaskedArray if there are masked pixels
        if isinstance(prcp, np.ma.MaskedArray):
            prcp = prcp.filled(fillvalue)

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

    # swap the y indices as the y resolution is always negative
    x_slice = slice(int(x1_px), int(math.ceil(x2_px)))
    y_slice = slice(int(y2_px), int(math.ceil(y1_px)))

    # slice the region of interest from the array
    logger.info('Taking slice (y %d:%d, x %d:%d) for member selection',
                y_slice.start, y_slice.stop, x_slice.start, x_slice.stop)
    prcp_roi = prcp[:, :, y_slice, x_slice].copy()

    # replace fillvalues with zeros for member selection
    prcp_roi[prcp_roi == fillvalue] = 0

    # ensemble member selection
    sums = prcp_roi.reshape(len(prcp_roi), -1).sum(1)
    p75 = np.percentile(sums, 75)
    member = np.abs(sums - p75).argmin().item()
    logger.info('Member sums are %s.', sums)
    logger.info('Selecting member %s.', member)

    # select member
    data = prcp[member]

    # prepare meta messages
    metadata = json.dumps({'file': os.path.basename(path), 'member': member})
    meta = config.DEPTH * [metadata]

    return regions.Region.from_mem(
        data=data,
        time=time,
        meta=meta,
        bands=(0, config.DEPTH),
        fillvalue=fillvalue.item(),
        geo_transform=config.GEO_TRANSFORM,
        projection=osr.GetUserInputAsWKT(str(config.PROJECTION))
    )


def rotate_steps():
    """
    Rotate steps stores.
    """
    # determine current store period
    period = load(os.path.join(config.STORE_DIR, config.NAME)).period
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
            path = os.path.join(tdir, latest)
            server.retrieve_to_path(name=latest, path=path)
            region = extract_region(path)
    except Exception:
        logger.exception('Error getting the steps data.')

        return

    # rotate the stores
    name = config.NAME
    path = os.path.join(config.STORE_DIR, name)
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
            'filename': os.path.join(config.LOG_DIR, 'steps_rotate.log')
        })
    logging.basicConfig(**kwargs)

    # run
    rotate_steps(**kwargs)
