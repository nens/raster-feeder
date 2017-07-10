# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Update Lizard API for modified raster stores.
"""
from os.path import join

import argparse
import logging
import sys

from . import common
from . import config

logger = logging.getLogger(__name__)


def get_parser():
    """Return argument parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        'raster_uuids',
        nargs='+',
        metavar='RASTER_UUID',
        help="Lizard raster endpoint UUID",
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
    )
    return parser


def main():
    # logging
    kwargs = vars(get_parser().parse_args())
    if kwargs['verbose']:
        logging.basicConfig(**{
            'stream': sys.stderr,
            'level': logging.INFO,
        })
    else:
        logging.basicConfig(**{
            'level': logging.INFO,
            'format': '%(asctime)s %(levelname)s %(message)s',
            'filename': join(config.LOG_DIR, 'touch_lizard.log')
        })

    for raster_uuid in kwargs['raster_uuids']:
        common.touch_lizard(raster_uuid)
