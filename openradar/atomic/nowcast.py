# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Stores latest nowcast into.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import argparse
import datetime
import ftplib
import json
import logging
import os
import shutil
import sys
import tempfile

import h5py
import numpy as np
import turn
from osgeo import osr

from raster_store import regions
from raster_store import stores

from openradar import config
from openradar import utils

WKT = osr.GetUserInputAsWKT(b'epsg:28992')

logger = logging.getLogger(__name__)


def fetch_latest_nowcast_h5():
    """ Return path to downloaded nowcastfile. """
    if not hasattr(config, 'FTP_NOWCAST'):
        logger.info('No nowcast found in ftp in config.')
        return

    credentials = config.FTP_NOWCAST
    try:
        source = ftplib.FTP(
            credentials['host'],
            credentials['user'],
            credentials['password'],
        )
        source.cwd(credentials['path'])
        nlst = source.nlst()
        if not nlst:
            logger.info('No files found on ftp.')
            return None
        target_name = sorted(nlst, reverse=True)[0]
        target_path = os.path.join(tempfile.mkdtemp(), target_name)
        with open(target_path, 'w') as target_file:
            source.retrbinary('RETR ' + target_name, target_file.write)
        source.quit()
    except:
        logging.exception('Error:')
        return None
    return target_path


def get_nowcast_region():
    """
    Get latest nowcast image as region.
    """
    # prepare
    geo_transform = utils.get_geo_transform()
    projection = WKT
    fmt = 'RAD_TF0005_R_PROG_%Y%m%d%H%M%S'
    fillvalue = np.finfo('f4').max.item()
    now = datetime.datetime.now().isoformat()
    # download
    path = fetch_latest_nowcast_h5()
    if path is None:
        return
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
            time.append(datetime.datetime.strptime(name, fmt))
    shutil.rmtree(os.path.dirname(path))
    # create a region
    region = regions.Region.from_mem(data=data,
                                     meta=meta,
                                     time=time,
                                     bands=bands,
                                     fillvalue=fillvalue,
                                     projection=projection,
                                     geo_transform=geo_transform)
    return region


def rotate_nowcast_stores(region):
    """
    Rotate the contents of the nowcast stores, filling the currently empty
    one with the latest data and removing the data from the currently
    loaded store.
    """
    # paths
    base = os.path.join(config.STORE_DIR, '5min')
    locker = turn.Locker()
    with locker.lock(resource='5min', label='nowcast'):
        old = stores.get(os.path.join(base, 'nowcast1'))
        new = stores.get(os.path.join(base, 'nowcast2'))
        if new:
            old, new = new, old
        new.update([region])
        if old:
            start, stop = old.period
            old.delete(start=start, stop=stop)


def command(verbose):
    """
    Update the nowcast store.
    """
    # logging
    if verbose:
        kwargs = {'stream': sys.stderr,
                  'level': logging.INFO}
    else:
        kwargs = {'level': logging.INFO,
                  'format': '%(asctime)s %(levelname)s %(message)s',
                  'filename': os.path.join(config.LOG_DIR, 'nowcast.log')}
    logging.basicConfig(**kwargs)
    logger.info('Nowcast procedure initiated.')

    # action
    region = get_nowcast_region()
    if region:
        rotate_nowcast_stores(region)
    logger.info('Nowcast procedure completed.')


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
    try:
        return command(**vars(get_parser().parse_args()))
    except:
        logger.exception('An execption occurred:')
