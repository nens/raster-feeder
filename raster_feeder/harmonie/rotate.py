# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Stores latest data in a rotating raster store group.
"""

from datetime import timedelta as Timedelta
from os.path import join

import argparse
import logging
import struct
import sys
import tarfile

import numpy as np
import pygrib
from osgeo import osr

from raster_store import load
from raster_store import regions

from ..common import rotate, touch_lizard, FTPServer
from . import config

logger = logging.getLogger(__name__)


def vapor_pressure_slope(temperature):
    """Slope of the vapor pressure curve in kPa / deg C, KNMI formula

    Source: FutureWater, J.M. Schuurmans & P. Droogers (2009), Penman-Monteith
    referentieverdamping: Inventarisatie beschikbaarheid en mogelijkheden tot
    regionalisatie.

    :param temperature: Temperature in degrees Celsius
    """
    T = temperature
    eps = 0.6107 * 10 ** (7.5 * T / (237.3 + T))
    s = (7.5 * 237.3) / ((237.3 + T) ** 2) * np.log(10) * eps
    return s


def makkink(radiation, temperature):
    """
    Computation of "referentie-gewasverdamping" in mm / h.

    Source: FutureWater, J.M. Schuurmans & P. Droogers (2009), Penman-Monteith
    referentieverdamping: Inventarisatie beschikbaarheid en mogelijkheden tot
    regionalisatie.

    :param radiation: Global radiation flux in W / m2
    :param temperature: Temperature in degrees Celsius
    """
    # see https://nl.wikipedia.org/wiki/Referentie-gewasverdamping

    lambd = 2.45e6  # heat of evaproation of water at 20 degC [J / kg]
    rho = 1e3       # specific weight of water [kg / m3]
    gamma = 0.066   # psychrometric constant [kPa / degC]

    s = vapor_pressure_slope(temperature)  # [kPa / degC]

    ET_ref = 0.65 * (s / (s + gamma)) * radiation  # [W / m2]
    return ET_ref / (rho * lambd) * (1000. * 3600.)  # [mm / h]


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
    with tarfile.open(fileobj=fileobj, mode="r") as archive:
        for member in archive:
            yield archive.extractfile(member).read()


def extract_regions(fileobj):
    """
    Return latest harmonie data as raster store region or None.

    :param fileobj: File object containing HARMONIE tarfile data.

    Extract the data per parameter as raster-store region. Note that it is
    assumed that the grib messages are in correct temporal order.
    """
    # group names and levels
    names = tuple(p['raster-store-group'] for p in config.PARAMETERS)

    # create a lookup-table for group names keyed by tuple of some parameters
    lut = {}
    for p in config.PARAMETERS:
        lut[
            p['indicatorOfParameter'],
            p['level'],
            p['timeRangeIndicator'],
            p['typeOfLevel'],
        ] = p['raster-store-group']

    # prepare containers for the result values
    data = {n: [] for n in names}
    time = {n: [] for n in names}

    # extract data with one pass of the tarfile
    logger.info('Extract data from tarfile.')
    for gribdata in unpack_tarfile(fileobj):
        for message in parse_gribdata(gribdata):
            indicatorOfParameter = message['indicatorOfParameter']
            timeRangeIndicator = message['timeRangeIndicator']
            try:
                n = lut[
                    indicatorOfParameter,
                    message['level'],
                    timeRangeIndicator,
                    message['typeOfLevel'],
                ]
            except KeyError:
                continue

            # there have been issues with the first message in CR, CS, CG they
            # have different shape, bounds, time and should not be in these
            # cumulative parameters anyway
            if message['numberOfValues'] != 90000:
                continue

            # time
            hours = message['endStep']
            time[n].append(message.analDate + Timedelta(hours=hours))

            # data row order is inverted compared to target raster storage
            data[n].append(message['values'][::-1].astype('f4'))

    # convert data lists to arrays
    for n in data:
        data[n] = np.array(data[n], dtype='f4')

    # populate the prcp time and data from cr
    time['harmonie-prcp'] = time['harmonie-cr']
    data['harmonie-prcp'] = data['harmonie-cr'].copy()

    # apply inverse cumsum operation on prcp data
    data['harmonie-prcp'][1:] -= data['harmonie-prcp'][:-1].copy()

    # apply inverse cumsum operation on cumulative radiation data
    time['harmonie-rad'] = time['harmonie-crad']
    data['harmonie-rad'] = data['harmonie-crad'].copy()
    data['harmonie-rad'][1:] = np.diff(data['harmonie-crad'], axis=0)
    data['harmonie-rad'] /= 3600.  # [J / h / m2] to [J / s / m2] (= [W / m2])

    data['harmonie-evap'] = makkink(data['harmonie-rad'],
                                    data['harmonie-temp'][1:] - 273.15)
    time['harmonie-evap'] = time['harmonie-rad']

    # return a region per parameter
    fillvalue = np.finfo('f4').max.item()
    projection = osr.GetUserInputAsWKT(str(config.PROJECTION))

    return {n: regions.Region.from_mem(
        time=time[n],
        bands=(0, len(time[n])),
        fillvalue=fillvalue,
        projection=projection,
        data=data[n],
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
        server = FTPServer(**config.FTP)
        latest = server.get_latest_match(config.PATTERN)
    except Exception:
        logger.exception('Error connecting to {}'.format(config.FTP['host']))
        return

    if latest is None:
        logger.info('No source files found on server, exiting.')
        return

    if current and latest <= current.strftime(config.FORMAT):
        logger.info('No update available, exiting.')
        return

    # download and process the file
    try:
        fileobj = server.retrieve_to_stream(name=latest)
    except Exception:
        logger.exception('Error retrieving {}'.format(latest))
        return

    # extract regions
    regions = extract_regions(fileobj)

    # rotate the stores
    for name, region in regions.items():
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
            'filename': join(config.LOG_DIR, 'harmonie_rotate.log')
        })

    # run
    rotate_harmonie(**kwargs)
