# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Update Lizard API for modified raster stores.
"""
from os.path import join

import argparse
import logging
import sys

import requests

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


def touch_lizard(raster_uuid):
    """Update the raster store metadata using the Lizard API."""
    url = config.LIZARD_TEMPLATE.format(raster_uuid=raster_uuid)
    headers = {
        'username': config.LIZARD_USERNAME,
        'password': config.LIZARD_PASSWORD,
    }

    resp = requests.post(url, headers=headers)
    short_uuid = raster_uuid.split('-')[0]
    if resp.ok:
        logger.info(
            "Metadata update succeeded for %s: %s",
            short_uuid,
            resp.json(),
        )
    else:
        logger.error(
            "Metadata update failed for %s: %s",
            short_uuid,
            resp.json(),
        )


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
        touch_lizard(raster_uuid)
