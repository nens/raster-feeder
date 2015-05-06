# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Stores latest radar into store.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from datetime import datetime as Datetime
import argparse
import json
import logging
import os
import sys

import h5py
import numpy as np
import turn

from osgeo import osr

from raster_store import regions
from raster_store import stores

from openradar import config
from openradar import periods
from openradar import utils

logger = logging.getLogger(__name__)

# stores and levels
GEO_TRANSFORM = utils.get_geo_transform()
LEVELS = {'r': 1, 'n': 2, 'a': 3}
EPOCH = Datetime.fromtimestamp(0).isoformat()
ROOT = config.STORE_DIR
NOW = Datetime.now().isoformat()  # or should we refresh more often?
WKT = osr.GetUserInputAsWKT(b'EPSG:28992')

NAMES = {'f': {'r': dict(group='5min', store='real1'),
               'n': dict(group='5min', store='near'),
               'a': dict(group='5min', store='after')},
         'h': {'r': dict(group='hour', store='real'),
               'n': dict(group='hour', store='near'),
               'a': dict(group='hour', store='after')},
         'd': {'r': dict(group='day', store='real'),
               'n': dict(group='day', store='near'),
               'a': dict(group='day', store='after')}}


def get_path_helper(timeframe, prodcode):
    """ Return pathhelper for combination. """
    consistent = utils.consistent_product_expected(prodcode=prodcode,
                                                   timeframe=timeframe)
    basedir = config.CONSISTENT_DIR if consistent else config.CALIBRATE_DIR
    code = config.PRODUCT_CODE[timeframe][prodcode]
    template = config.PRODUCT_TEMPLATE
    return utils.PathHelper(basedir=basedir, code=code, template=template)


def get_mtime(path):
    """ return mtime as datetime object. """
    return Datetime.fromtimestamp(os.path.getmtime(path)).isoformat()


def get_contents(path):
    """ Return data, meta dictionary. """
    with h5py.File(path, 'r') as h5:
        data = h5['precipitation'][:]
        meta = dict(h5.attrs)
        fillvalue = h5.attrs['fill_value'].item()

        # make json serializable already
        for k, v in meta.items():
            if hasattr(v, 'tolist'):
                meta[k] = v.tolist()
        return dict(data=data, meta=meta, fillvalue=fillvalue)


class Store(object):
    """ An autothrottling store. """
    def __init__(self, timeframe, prodcode):
        self.timeframe = timeframe
        self.prodcode = prodcode

        # stores
        self.names = NAMES[timeframe][prodcode]
        group_path = os.path.join(config.STORE_DIR, self.names['group'])
        store_path = os.path.join(group_path, self.names['store'])
        self.store = stores.get(store_path)
        self.group = stores.get(group_path)

        # others
        self.helper = get_path_helper(timeframe, prodcode)
        self.level = LEVELS[prodcode]

    def reset(self, datetime):
        """ Init band lookup table for current chunk. """
        band = self.store.select_bands(datetime)[0]
        depth = self.store.max_depth
        chunk = band // depth
        start = chunk * depth
        stop = start + depth
        bands = xrange(0, stop - start)

        # make loop-up table for bands and create sources dictionary
        times = self.store.get_time_for_bands((start, stop))
        self.bands = dict(zip(times, bands))
        self.meta = self.group.get_meta_direct(times[0], times[-1])

        # put here datetime: mtime, path
        self.sources = {}

    # meta caching and source queueing
    def flush(self):
        """ Load all accepted products into store. """
        if not self.sources:
            return
        # create region
        start = min(self.sources)
        stop = max(self.sources)
        size = stop - start + 1
        shape = size, 490, 500
        bands = 0, shape[0]
        data = np.ones(shape, self.store.dtype) * self.store.fillvalue
        meta = size * [None]
        time = sorted(t for t, b in self.bands.items() if start <= b <= stop)
        region = regions.Region.from_mem(data=data,
                                         meta=meta,
                                         time=time,
                                         bands=bands,
                                         fillvalue=None,
                                         projection=WKT,
                                         geo_transform=GEO_TRANSFORM)

        # populate it
        for band, source in self.sources.items():

            contents = get_contents(source['path'])

            if region.box.fillvalue is None:
                region.fillvalue = contents['fillvalue']

            region.box.data[band - start] = contents['data']

            meta = contents['meta']
            meta.update({'stored': NOW,
                         'modified': source['mtime'],
                         'prodcode': self.prodcode})
            region.meta[band - start] = json.dumps(meta)

        logger.info('Store {} sources'.format(len(self.sources)))
        self.store.update([region])

    def process(self, period):
        """
        Controls flushing and filters according to timeframe.
        on start: init according to datetime
        on edge: init and flush
        on finish: flush
        """
        datetimes = (d
                     for d in period
                     if self.timeframe in utils.get_valid_timeframes(d))
        # grab first datetime if any, reset band index, meta accordingly
        try:
            first = datetimes.next()
        except StopIteration:
            return
        self.reset(first)
        self.consider(first)
        for datetime in datetimes:
            if datetime not in self.bands:
                self.flush()
                self.reset(datetime)
            self.consider(datetime)
        self.flush()

    def consider(self, datetime):
        """ Consider a matching product for loading. """
        # logging fields
        fields = {'t': self.timeframe, 'p': self.prodcode, 'd': datetime}

        # about the group for this datetime
        group_meta_json = self.meta.get(datetime)
        group_meta = json.loads(group_meta_json) if group_meta_json else {}

        group_level = LEVELS.get(group_meta.get('prodcode'), -1)
        group_mtime = group_meta.get('stored', EPOCH)

        # don't even think about inferior products
        if self.level < group_level:
            logger.debug('store has better: {d} {t} {p}'.format(**fields))
            return

        # discard unavailable products
        path = self.helper.path(datetime)
        try:
            mtime = get_mtime(path)
        except OSError:
            logger.debug('not on disk: {d} {t} {p}'.format(**fields))
            return

        # discard older products if the levels are the same
        if self.level == group_level:
            if mtime <= group_mtime:
                logger.debug('present: {d} {t} {p}'.format(**fields))
                return

        # flush when logical
        logger.debug('queueing: {d} {t} {p}'.format(**fields))
        self.sources[self.bands[datetime]] = {'path': path, 'mtime': mtime}

    def __getitem__(self, datetime):
        if self.meta_period is None:
            self.fetch_meta(datetime)
        if datetime < self.meta_period[0] or datetime > self.meta_period[1]:
            self.fetch_meta(datetime)
        return self.meta_dict.get(datetime, {})

    def update(self, source):
        """ Append source to queue and flush if queue becomes to big. """
        self.queue.append(source)
        if len(self.queue) == 128:
            self.flush()


def command(text, verbose):
    """ No doc yet. """
    # logging
    if verbose:
        kwargs = {'stream': sys.stderr,
                  'level': logging.INFO}
    else:
        kwargs = {'level': logging.INFO,
                  'format': '%(asctime)s %(levelname)s %(message)s',
                  'filename': os.path.join(config.LOG_DIR, 'store.log')}
    logging.basicConfig(**kwargs)

    # storing
    logger.info('Store procedure initiated.')

    period = periods.Period(text)
    locker = turn.Locker()
    for timeframe in 'fhd':
        for prodcode in 'r':  # notice reversed order
            resource = NAMES[timeframe][prodcode]['group']
            with locker.lock(resource=resource, label='store'):
                kwargs = {'timeframe': timeframe, 'prodcode': prodcode}
                store = Store(**kwargs)
                store.process(period)

    logger.info('Store procedure completed.')


def get_parser():
    """ Return argument parser. """
    parser = argparse.ArgumentParser(
        description=__doc__
    )
    parser.add_argument(
        'text',
        metavar='PERIOD',
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
        logger.exception('An exception occurred:')
