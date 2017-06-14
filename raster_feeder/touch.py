# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Update Lizard API for modified raster stores.
"""
import argparse
import logging

import requests

from .config import LIZARD_USERNAME, LIZARD_PASSWORD

logger = logging.getLogger(__name__)
RASTERS_SET_META_URL_TEMPLATE = 'https://demo.lizard.net/api/v2/rasters/{raster_uuid}/set_meta/'  # noqa


def get_parser():
    """Return argument parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        'raster_uuids',
        nargs='+',
        type=str,
        metavar='RASTER_UUID',
        help="Lizard raster endpoint UUID",
    )
    return parser


def touch_lizard(raster_uuid):
    """Update the Lizard raster store metadata."""
    url = RASTERS_SET_META_URL_TEMPLATE.format(raster_uuid=raster_uuid)
    headers = {'username': LIZARD_USERNAME, 'password': LIZARD_PASSWORD}

    resp = requests.post(url, headers=headers)
    if resp.ok:
        logger.info("Metadata update succeeded: %s", resp.json())
    else:
        logger.error("Metedata update failed: %s", resp.json())


def main():
    args = get_parser().parse_args()
    for raster_uuid in args.raster_uuids:
        touch_lizard(raster_uuid)
