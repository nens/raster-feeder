# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Stores latest harmonie in a rotating raster store group.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from datetime import timedelta as Timedelta
from os.path import join

import argparse
import contextlib
import ftplib
import logging
import shutil
import struct
import sys
import tarfile
import tempfile

import numpy as np
import pygrib
import redis
import turn
from osgeo import osr

from raster_store import cache
from raster_store import load
from raster_store import regions

from . import config

logger = logging.getLogger(__name__)

# mtime caching
cache.client = redis.Redis(host=config.REDIS_HOST, db=config.REDIS_DB)


class Grib(object):
    """
    Grib message parser.

    Data should be the bytes of a GRIB file. The parser slices the data into
    grib messages using the message size indicators from the data.

    Currently only rain intensity messages are yielded.
    """
    def __init__(self, data):
        self.data = data

    def __iter__(self):
        """ Return generator of message objects. """
        start = 0
        while start != -1:
            # grib edition 1 uses bytes 5-7 to indicate message size
            indicator = chr(0) + self.data[start + 4:start + 7]
            size = struct.unpack('>I', indicator)[0]

            end = start + size
            message = pygrib.fromstring(self.data[start:end])

            # filter for precipitation intensity here
            code = message['indicatorOfParameter']
            level = message['level']
            if code == 61 and level == 456:
                yield message

            start = self.data.find(bytes('GRIB'), end)


class Harmonie(object):
    """ KNMI HARMONIE tarfile reader. """
    def __init__(self, path):
        self.path = path

    def __iter__(self):
        """ Return generator of Grib objects. """
        with tarfile.open(self.path, mode="r:gz") as archive:
            for member in archive:
                yield Grib(archive.extractfile(member).read())


class Downloader(object):
    def __init__(self):
        """ Connect and remember latest file. """
        logger.info('Connecting to "{}".'.format(config.FTP['host']))
        self.connection = ftplib.FTP(
            host=config.FTP['host'],
            user=config.FTP['user'],
            passwd=config.FTP['password'],
        )
        self.connection.cwd(config.FTP['path'])
        self.latest = sorted(self.connection.nlst())[-1]

    @contextlib.contextmanager
    def download(self):
        """ Return path to downloaded nowcastfile, if any. """
        target_dir = tempfile.mkdtemp()
        target_path = join(target_dir, self.latest)

        # retrieve, yield and cleanup
        logger.info('Downloading {} from FTP.'.format(self.latest))
        try:
            with open(target_path, 'w') as target_file:
                self.connection.retrbinary(
                    'RETR ' + self.latest,
                    target_file.write,
                )
            yield target_path
        except:
            logging.exception('Error:')

        # no need for finally, because of the catch-all
        shutil.rmtree(target_dir)

    def quit(self):
        """ Close connection. """
        self.connection.quit()


# class Downloader(object):
#     """ Dummy downloader for debugging purposes. """
#     def __init__(self):
#         self.latest = None

#     @contextlib.contextmanager
#     def download(self):
#         """ Return path to downloaded nowcastfile, if any. """
#         path = 'harm36_v1_ned_surface_2017041006.tgz'
#         logger.info('Using dummy file "{}".'.format(path))
#         yield path

#     def quit(self):
#         pass


def get_region(current=None):
    """
    Return latest harmonie data as raster store region or None.

    :param current: Datetime

    Loads and processes the latest data into a raster-store region. Does not
    fetch the data if the datetime of the latest available data corresponds to
    the current parameter.
    """
    # login
    try:
        downloader = Downloader()
    except:
        logging.exception('Error:')
        return

    if current and downloader.latest == current.strftime(config.FORMAT):
        logger.info('No update available, exiting.')
        downloader.quit()
        return

    time = []
    shape = 49, 300, 300
    fillvalue = np.finfo('f4').max.item()
    projection = osr.GetUserInputAsWKT(str(config.PROJECTION))
    data = np.full(shape, fillvalue, dtype='f4')

    # download
    with downloader.download() as path:
        logger.info('Constructing region from tarfile.')
        harmonie = Harmonie(path)
        for i, grib in enumerate(harmonie):
            message = next(iter(grib))

            # time
            hours = message['startStep']
            time.append(message.analDate + Timedelta(hours=hours))

            # data is upside down and in millimeter / second
            data[i] = message['values'][::-1] * 3600

    # create a region
    region = regions.Region.from_mem(
        data=data,
        time=time,
        bands=(0, 49),
        fillvalue=fillvalue,
        projection=projection,
        geo_transform=config.GEO_TRANSFORM,
    )

    downloader.quit()
    return region


def rotate_harmonie():
    """
    Rotate harmonie stores.
    """
    group_path = join(config.STORE_DIR, config.GROUP_NAME)

    # see if there is an update
    period = load(group_path + '.json').period
    if period is None:
        current = None
    else:
        current = period[0]
    region = get_region(current)
    if region is None:
        return

    # the actual rotating
    logger.info('Starting store rotation.')
    locker = turn.Locker(host=config.REDIS_HOST, db=config.REDIS_DB)
    with locker.lock(resource='harmonie', label='harmonie'):
        old = load(join(group_path, config.STORE_NAMES[0]))
        new = load(join(group_path, config.STORE_NAMES[1]))
        if new:
            old, new = new, old
        new.update([region])
        if old:
            start, stop = old.period
            old.delete(start=start, stop=stop)

    logger.info('Rotate complete.')


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
    logging.basicConfig(**kwargs)

    # run
    rotate_harmonie(**kwargs)
