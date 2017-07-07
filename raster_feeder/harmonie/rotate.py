# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Stores latest data in a rotating raster store group.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from datetime import timedelta as Timedelta
from os.path import join

import argparse
import ftplib
import io
import logging
import struct
import sys
import tarfile

import numpy as np
import pygrib
from osgeo import osr

from raster_store import load
from raster_store import regions

from ..common import rotate
from . import config

logger = logging.getLogger(__name__)


def parse_gribdata(gribdata):
    """
    Return generator of message objects.

    Data should be the bytes of a GRIB file. The parser slices the data into
    grib messages using the message size indicators from the data.

    Currently only rain intensity messages are yielded.
    """
    start = 0
    while start != -1:
        # grib edition 1 uses bytes 5-7 to indicate message size
        indicator = chr(0) + gribdata[start + 4:start + 7]
        size = struct.unpack('>I', indicator)[0]

        end = start + size
        message = pygrib.fromstring(gribdata[start:end])

        yield message
        start = gribdata.find(bytes('GRIB'), end)


def unpack_tarfile(fileobj):
    """
    Return generator of gribfile bytestrings.

    :param fileobj: File object containing HARMONIE tarfile data.
    """
    with tarfile.open(fileobj=fileobj, mode="r:gz") as archive:
        for member in archive:
            yield archive.extractfile(member).read()


def download(current=None):
    """
    Return file object or None of no update is available.

    :param current: Datetime of current available data
    """
    # connect
    logger.info('Connecting to "{}".'.format(config.FTP['host']))
    connection = ftplib.FTP(
        host=config.FTP['host'],
        user=config.FTP['user'],
        passwd=config.FTP['password'],
    )
    connection.cwd(config.FTP['path'])

    # check for update
    latest = sorted(connection.nlst())[-1]
    if current and latest == current.strftime(config.FORMAT):
        result = None
    else:
        logger.info('Downloading {} from FTP.'.format(latest))
        result = io.BytesIO()
        connection.retrbinary('RETR ' + latest, result.write)
        result.seek(0)
    connection.quit()
    return result


# def download(current=None):
    # """ Dummy downloader for debugging purposes. """
    # # return open file object
    # path = 'harm36_v1_ned_surface_2017060600.tgz'
    # logger.info('Using dummy file "{}".'.format(path))
    # return open(path)


def extract_regions(fileobj):
    """
    Return latest harmonie data as raster store region or None.

    :param fileobj: File object containing HARMONIE tarfile data.

    Extract the data per parameter as raster-store region. Note that it is
    assumed that the grib messages are in correct temporal order.
    """
    # group names and levels
    names = tuple(p['group'] for p in config.PARAMETERS)
    levels = tuple(p['level'] for p in config.PARAMETERS)

    # create a lookup-table for group names by level
    lut = dict(zip(levels, names))

    # prepare containers for the result values
    data = {n: [] for n in names}
    time = {n: [] for n in names}

    # extract data with one pass of the tarfile
    logger.info('Extract data from tarfile.')
    for gribdata in unpack_tarfile(fileobj):
        for message in parse_gribdata(gribdata):
            if message['indicatorOfParameter'] != 61:  # parameter code
                continue
            level = message['level']
            try:
                n = lut[level]
            except IndexError:
                continue

            # time
            hours = message['startStep']
            time[n].append(message.analDate + Timedelta(hours=hours))

            # data is upside down
            data[n].append(message['values'][::-1])

    # return a region per parameter
    fillvalue = np.finfo('f4').max.item()
    projection = osr.GetUserInputAsWKT(str(config.PROJECTION))

    return {n: regions.Region.from_mem(
        time=time[n],
        bands=(0, len(time[n])),
        fillvalue=fillvalue,
        projection=projection,
        data=np.array(data[n]),
        geo_transform=config.GEO_TRANSFORM,
    ) for n in data}


def rotate_harmonie():
    """
    Rotate harmonie stores.
    """
    # determine current store period
    period = load(join(config.STORE_DIR, config.PERIOD_REFERENCE)).period
    if period is None:
        current = None
    else:
        current = period[0]

    # retrieve updated data
    try:
        fileobj = download(current)
    except:
        logger.exception('Error:')
        return
    if fileobj is None:
        logger.info('No update available, exiting.')
        return

    # extract regions
    regions = extract_regions(fileobj)

    # rotate the stores
    for name, region in regions.items():
        path = join(config.STORE_DIR, name)
        rotate(path=path, region=region, resource=name)


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
            'filename': join(config.LOG_DIR, 'harmonie_rotate.log')
        })

    # run
    rotate_harmonie(**kwargs)
