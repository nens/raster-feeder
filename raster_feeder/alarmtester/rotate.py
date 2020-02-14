# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Stores latest data in a rotating raster store group.
"""

from datetime import timedelta as Timedelta
from datetime import datetime as Datetime

import argparse
import logging
import os
import sys

import numpy as np

from osgeo import osr
from raster_store import regions

from ..common import rotate, touch_lizard
from . import config

logger = logging.getLogger(__name__)


def rotate_alarmtester():
    """
    Rotate alarm tester stores.
    """
    # obtain origin two hours before now and rounded to 5 minutes
    now = Datetime.utcnow()
    origin = now - Timedelta(hours=2, minutes=now.minute % 5,
                             seconds=now.second, microseconds=now.microsecond)

    # generate the timepoints from that
    time = [origin + i * Timedelta(minutes=5) for i in range(config.DEPTH)]

    # generate data
    xp, yp = np.array(config.VALUES).T
    values = np.interp(np.linspace(-2, 6, config.DEPTH, endpoint=True),
                       xp, yp)
    data = np.empty((config.DEPTH, 2, 4), dtype='f4')
    data[:] = values[:, np.newaxis, np.newaxis]

    region = regions.Region.from_mem(
        time=time,
        bands=(0, config.DEPTH),
        fillvalue=np.finfo('f4').max.item(),
        projection=osr.GetUserInputAsWKT(str(config.PROJECTION)),
        data=data,
        geo_transform=config.GEO_TRANSFORM,
    )

    # rotate the stores
    path = os.path.join(config.STORE_DIR, config.NAME)
    rotate(path=path, region=region, resource=config.NAME)

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
            'filename': os.path.join(config.LOG_DIR, 'alarmtester_rotate.log')
        })

    # run
    rotate_alarmtester(**kwargs)
