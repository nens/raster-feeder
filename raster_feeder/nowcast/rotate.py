# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Stores latest data in a rotating raster store group.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from os.path import dirname, join
from datetime import datetime as Datetime
import argparse
import ftplib
import json
import logging
import shutil
import sys
import tempfile

from osgeo import osr
import h5py
import numpy as np

from raster_store import regions

from ..common import rotate
from ..common import touch_lizard
from . import config

logger = logging.getLogger(__name__)


def fetch_latest_nowcast_h5():
    """ Return path to downloaded nowcastfile. """
    logger.info('Connecting to "{}".'.format(config.FTP['host']))
    connection = ftplib.FTP(
        host=config.FTP['host'],
        user=config.FTP['user'],
        passwd=config.FTP['password'],
    )
    connection.cwd(config.FTP['path'])

    nlst = [n for n in connection.nlst()
            if n.startswith('RAD_TF0005_R_PROG_')]

    if not nlst:
        raise ValueError('No nowcast files found on FTP server.')

    target_name = sorted(nlst)[-1]
    target_path = join(tempfile.mkdtemp(), target_name)
    with open(target_path, 'w') as target_file:
        connection.retrbinary('RETR ' + target_name, target_file.write)
    connection.quit()

    return target_path


def get_nowcast_region():
    """
    Get latest nowcast image as region.
    """
    # prepare
    geo_transform = config.GEO_TRANSFORM
    projection = osr.GetUserInputAsWKT(str(config.PROJECTION))
    fmt = 'RAD_TF0005_R_PROG_%Y%m%d%H%M%S'
    fillvalue = np.finfo('f4').max.item()
    now = Datetime.now().isoformat()
    # download
    path = fetch_latest_nowcast_h5()
    logger.debug('Received nowcastfile {}'.format(path))
    # read
    with h5py.File(path, 'r') as h5:
        images = [k for k in h5.keys() if k.startswith('image')]
        images.sort(key=lambda n: int(n[5:]))
        shape = (len(images),) + h5[images[0]]['image_data'].shape
        data = np.empty(shape, 'f4')
        bands = 0, len(images)
        time = []
        meta = []
        for i, image in enumerate(images):
            data[i] = h5[image]['image_data'][:] / 100
            name = h5[image].attrs['image_product_name']
            meta.append(json.dumps({'product': name, 'stored': now}))
            time.append(Datetime.strptime(name, fmt))
    shutil.rmtree(dirname(path))

    # retrun as region
    return regions.Region.from_mem(
        data=data,
        meta=meta,
        time=time,
        bands=bands,
        fillvalue=fillvalue,
        projection=projection,
        geo_transform=geo_transform
    )


def rotate_nowcast():
    """
    Rotate nowcast stores.
    """
    # retrieve updated data
    try:
        region = get_nowcast_region()
    except Exception:
        logger.exception('Error getting the nowcast data.')
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
            'filename': join(config.LOG_DIR, 'nowcast_rotate.log')
        })
    logging.basicConfig(**kwargs)

    # run
    rotate_nowcast(**kwargs)
