# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Stores latest steps in a rotating raster store group.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

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

from raster_feeder.steps import config
from raster_feeder.common import create_tumbler
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta

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
    inv_geo_transform = gdal.InvGeoTransform(config.GEO_TRANSFORM)[1]
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
    member = np.abs(sums - p75).argmin()
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


def rotate_steps_local():
    """
    Rotate steps stores.
    """
    # process the files in order -- save state
    # clean folder from steps-init operation
    if os.path.isdir(os.path.join(config.STORE_DIR, config.NAME, 'steps1')):
        shutil.rmtree(os.path.join(config.STORE_DIR, config.NAME, 'steps1'))
    if os.path.isdir(os.path.join(config.STORE_DIR, config.NAME, 'steps2')):
        shutil.rmtree(os.path.join(config.STORE_DIR, config.NAME, 'steps2'))

    # run init - create a empty tumbler
    create_tumbler(
        path=os.path.join(config.STORE_DIR, config.NAME),
        depth=config.DEPTH,
        dtype='f4',
        delta=Timedelta(minutes=10),
        projection=config.PROJECTION,
        geo_transform=config.GEO_TRANSFORM,
        origin=Datetime(year=2000, month=1, day=1),
    )

    # delete steps2 - we do not need that
    shutil.rmtree(os.path.join(config.STORE_DIR, config.NAME, 'steps2'))

    # sort files by date
    files = sorted([
        f for f in os.listdir(config.LOCAL_SOURCE_DIR)
        if os.path.isfile(os.path.join(config.LOCAL_SOURCE_DIR, f))
        if f.split('.')[-1] == 'nc'
    ])

    for file in files:
        try:
            # name for raster -- this will be used in '.gif' file
            file_timestamp = file.split('.')[0]

            # make a new raster from step1
            rasterdir_path = os.path.join(config.LOCAL_STORE_DIR, file_timestamp)
            if os.path.isdir(rasterdir_path):
                shutil.rmtree(rasterdir_path)
            shutil.copytree(
                os.path.join(config.STORE_DIR, config.NAME, 'steps1'),
                rasterdir_path
            )

            # update region
            raster_store = load(rasterdir_path)
            region = extract_region(os.path.join(config.LOCAL_SOURCE_DIR, file))
            raster_store.update([region])
        except Exception:
            logger.exception('Error processing files.')
            continue

def rotate_steps_local_single_raster():
    """
    Rotate steps stores.
    """
    # process the files in order -- save state
    # clean folder from steps-init operation
    if os.path.isdir(os.path.join(config.STORE_DIR, config.NAME, 'steps1')):
        shutil.rmtree(os.path.join(config.STORE_DIR, config.NAME, 'steps1'))
    if os.path.isdir(os.path.join(config.STORE_DIR, config.NAME, 'steps2')):
        shutil.rmtree(os.path.join(config.STORE_DIR, config.NAME, 'steps2'))

    # run init - create a empty tumbler
    create_tumbler(
        path=os.path.join(config.STORE_DIR, config.NAME),
        depth=config.DEPTH,
        dtype='f4',
        delta=Timedelta(minutes=10),
        projection=config.PROJECTION,
        geo_transform=config.GEO_TRANSFORM,
        origin=Datetime(year=2000, month=1, day=1),
    )

    # delete steps2 - we do not need that
    shutil.rmtree(os.path.join(config.STORE_DIR, config.NAME, 'steps2'))

    # sort files by date
    files = sorted([
        f for f in os.listdir(config.LOCAL_SOURCE_DIR)
        if os.path.isfile(os.path.join(config.LOCAL_SOURCE_DIR, f))
        if f.split('.')[-1] == 'nc'
    ])

    # make a new raster from step1
    raster_name = files[0].split('.')[0].split('_')[1]
    rasterdir_path = os.path.join(config.LOCAL_STORE_DIR, 'single_raster_{}'.format(raster_name))
    if os.path.isdir(rasterdir_path):
        shutil.rmtree(rasterdir_path)
    shutil.copytree(
        os.path.join(config.STORE_DIR, config.NAME, 'steps1'),
        rasterdir_path
    )
    raster_store = load(rasterdir_path)

    for file in files:
        try:
            region = extract_region(os.path.join(config.LOCAL_SOURCE_DIR, file))
            raster_store.update([region])
        except Exception:
            logger.exception('Error processing files.')
            continue

def get_parser():
    """ Return argument parser. """
    parser = argparse.ArgumentParser(
        description=__doc__
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
    )
    parser.add_argument(
        '-sr', '--singleraster',
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

    if kwargs.get('singleraster'):
        rotate_steps_local_single_raster()
    else:
        rotate_steps_local()


if __name__ == '__main__':
    sys.exit(main())


"""  
1. - Create file docker-compose.override.yml and add: 

version: '3'

services:
    lib:
      volumes:
        - /home/jose/Downloads/debug-eggs:/pycharm-helpers
        - /home/jose/Desktop/Rainfields/ncfiles:/code/var/local/source
        - /home/jose/Desktop/Rainfields/ncrasters:/code/var/local/rasters
        
2. - Place nc files in local:
   /home/jose/Desktop/Rainfields/ncfiles
   
3. - Select option write in a single raster or generate a raster per nc:

  -sr: single raster
  -no argument: a raster per nc.
  
4. - Make sure that this exists in your local
    - /home/jose/Desktop/Rainfields/ncfiles:/code/var/local/source
    - /home/jose/Desktop/Rainfields/ncrasters:/code/var/local/rasters          
"""
