# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
# -*- coding: utf-8 -*-
"""
Export aggregated data from an NRR raster-store into separate GeoTIFF files.
"""
from contextlib import contextmanager
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta
from os.path import exists, join

import argparse
import logging
import os
# import time

from osgeo import gdal
from raster_store import datasets
from raster_store import load
import numpy as np

from . import config

logger = logging.getLogger(__name__)

GEO_TRANSFORM = -110000, 1000, 0, 700000, 0, -1000
PROJECTION = "EPSG:28992"
NO_DATA_VALUE = -9999
WIDTH = 500
HEIGHT = 490

REQUEST = {
    'bbox': (-110000, 210000, 390000, 700000),
    'projection': PROJECTION,
    "height": HEIGHT,
    "width": WIDTH,
    'mode': 'vals',
}

KWARGS = {
    'geo_transform': GEO_TRANSFORM,
    'no_data_value': NO_DATA_VALUE,
    'projection': PROJECTION,
}

DTYPE = 'f4'
GTIFF = gdal.GetDriverByName('GTiff')


def parse_period(text):
    start, *stops = text.split('-')
    return (
        Datetime.strptime(start, '%Y%m%d%H%M'),
        Datetime.strptime(stops[0] if stops else start, '%Y%m%d%H%M'),
    )


def get_store(product):
    """Return a raster store."""
    path = join(config.STORE_DIR, product)
    return load(path)


def get_snapped(datetime, step, offset):
    """
    Return snapped datetime to a timeseries defined by delta and offset

    Args:
        datetime: Datetime instance to snap
        step: Timedelta instance defining the period
        offset: Timedelta instance defining the offset
    """
    total = datetime.timestamp() - offset.total_seconds()
    delta = step.total_seconds()
    return Datetime.fromtimestamp((total // delta) * delta)


def get_datetimes(store, period, size):
    """Return groups of dates of `size`. It is assumed that the start is
    already rounded to timedelta.
    """
    # snap start
    start, end = period

    delta = store.timedelta
    if delta == Timedelta(days=1):
        offset = Timedelta(hours=8)  # NRR days start at 8.
    else:
        offset = Timedelta()
    step = size * delta
    datetime = get_snapped(datetime=start, step=step, offset=offset)

    # return groups that completely fit in store period
    lo, hi = store.period
    offsets = [x * delta for x in range(1 - size, 1)]
    while datetime <= end:
        datetimes = [datetime + offset for offset in offsets]
        if datetimes[0] > lo and datetimes[-1] < hi:
            yield datetimes
        datetime += step


@contextmanager
def get_dataset(store, datetimes):
    """Return dataset of dataset tuples.

    Args:
        store: Store object Storeinstance.
        datetimes: Datetimes to query and accumulate.
    """
    values = np.full((1, HEIGHT, WIDTH), NO_DATA_VALUE, dtype=DTYPE)
    active = (values != NO_DATA_VALUE)
    for datetime in datetimes:
        # get data
        request = {"start": datetime, "stop": datetime, **REQUEST}
        data = store.get_data(**request)
        values_in = data["values"]
        active_in = values_in != data["no_data_value"]

        # sum values that are active in both arrays
        both_active = (active & active_in)
        values[both_active] += values_in[both_active]

        # set values that are active only in the new array
        new_active = (~active & active_in)
        values[new_active] = values_in[new_active]
        active[new_active] = True

    with datasets.Dataset(values, **KWARGS) as dataset:
        yield dataset


def export(product, period, path, size):
    store = get_store(product)
    for datetimes in get_datetimes(store=store, period=period, size=size):
        # destination path
        tif_name = datetimes[-1].strftime('%Y%m%d%H%M.tif')
        tif_dir = join(path, tif_name[0:4], tif_name[4:6], tif_name[6:8])
        tif_path = join(tif_dir, tif_name)
        os.makedirs(tif_dir, exist_ok=True)

        if exists(tif_path):
            print(f'Skip {tif_path}')
            continue

        with get_dataset(store=store, datetimes=datetimes) as dataset:
            print(f'Save {tif_path}')
            GTIFF.CreateCopy(tif_path, dataset, options=['compress=deflate'])


def get_parser():
    """ Return argument parser. """
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        'product',
        metavar='STORE',
        help='Relative path of store to export from, e.g. `5min/ultimate`.',
    )
    parser.add_argument(
        'period',
        type=parse_period,
        help=(
            'Time period, e.g. `202001011800` or `202001012000-202001012200`.'
        ),
    )
    parser.add_argument(
        'path',
        metavar='DESTINATION',
        help='Destination directory.',
    )
    parser.add_argument(
        '--aggregation-size', '-a',
        dest='size',
        type=int,
        default=1,
        help='Number of frames to sum into single export file.',
    )
    return parser


def main():
    """ Call move with args from parser. """
    kwargs = vars(get_parser().parse_args())
    export(**kwargs)
