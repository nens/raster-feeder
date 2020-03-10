# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Store a single steps file in a fresh raster store.
"""

from datetime import datetime as Datetime
from datetime import timedelta as Timedelta
from os.path import basename

import argparse
import logging

from raster_store import stores

from . import config
from . import rotate

logger = logging.getLogger(__name__)


def create_steps_store(path):
    """
    Create a raster store suitable for steps data.

    :param path: path to group to be created
    :type path: str
    """
    # properties
    kwargs = {
        "dtype": "f4",
        "delta": Timedelta(minutes=10),
        "projection": config.PROJECTION,
        "geo_transform": config.GEO_TRANSFORM,
        "origin": Datetime(year=2000, month=1, day=1),
    }

    # create store
    create_kwargs = {'path': path}
    create_kwargs.update(kwargs)
    store = stores.Store.create(path, **kwargs)

    # create storages
    store.create_storage((config.DEPTH, 1))
    store.create_storage((config.DEPTH, config.DEPTH))

    # create aggregation
    store.create_aggregation('topleft', (config.DEPTH, 1))

    return store


def get_parser():
    """ Return argument parser. """
    parser = argparse.ArgumentParser(
        description=__doc__
    )
    parser.add_argument(
        "source_path",
        metavar="SOURCE",
        help="Source steps netCDF file."
    )
    parser.add_argument(
        "target_path",
        metavar="TARGET",
        help="Target raster-store (will be created).",
    )
    parser.add_argument(
        "percentile",
        type=int,
        help="Percentile number for the member selection."
    )
    return parser


def single(source_path, target_path, percentile):
    """
    Convert steps netcdf at source_path to newly created raster store at
    target_path.
    """
    logger.info(f"Creating store from {basename(source_path)}")
    store = create_steps_store(target_path)
    region = rotate.extract_region(source_path, percentile)
    store.update([region])


def main():
    """ Call command with args from parser. """
    kwargs = vars(get_parser().parse_args())

    # create logfile next to store
    logging_path = kwargs["target_path"] + ".log"
    logging_kwargs = {
        "filename": logging_path,
        "level": logging.INFO,
        "format": "%(message)s",
    }
    logging.basicConfig(**logging_kwargs)

    single(**kwargs)
