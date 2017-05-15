# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Create the necessary radar stores if they do not yet exist and create
wrapper configurations per timeframe.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from os.path import join, exists

import argparse
import datetime
import json
import logging
import os
import sys

from osgeo import osr

from raster_store import stores

from . import config

logger = logging.getLogger(__name__)

DELTAS = {'5min': datetime.timedelta(minutes=5),
          'hour': datetime.timedelta(hours=1),
          'day': datetime.timedelta(days=1)}

# the depths here aim at fast storage and merging
# the offloading is more heavy and should definitely happen at night
DEPTHS = {'5min': {'real1':      (1,   12),   # arrives every 5 minutes
                   'real2':     (12,  288),   # move here every hour
                   'near':      (12,  288),   # arrives every hour
                   'after':     (72,  288),   # arrives once a day
                   'ultimate':  (72,  288),   # arrives once a day
                   'merge':     (72,  576),   # merge at night
                   'final':    (512, 1024)},  # offload once a day at night
          'hour': {'real':       (1,   24),   # arrives every hour
                   'near':       (1,   24),   # arrives every hour
                   'after':     (24,   24),   # arrives once a day
                   'ultimate':  (24,   24),   # arrives once a day
                   'merge':     (24,  576),   # merge at night
                   'final':    (512, 1024)},  # offload once a week at night
          'day':  {'real':       (1,   24),   # arrives once a day
                   'near':       (1,   24),   # arrives once a day
                   'after':      (1,   24),   # arrives once a day
                   'ultimate':   (1,   24),   # arrives once a day
                   'merge':      (6,  576),   # merge at night
                   'final':    (512, 1024)}}  # offload once a month at night

WKT = osr.GetUserInputAsWKT(str(config.PROJECTION))

ORIGINS = {'day': datetime.datetime(2000, 1, 1, 8),
           'hour': datetime.datetime(2000, 1, 1, 9),
           '5min': datetime.datetime(2000, 1, 1, 8, 5)}

KWARGS = {'dtype': 'f4',
          'projection': WKT,
          'geo_transform': config.GEO_TRANSFORM,
          'h5opts': {'scaleoffset': 2, 'compression': 'lzf'}}

ORDERING = {
    '5min': ('nowcast2', 'nowcast1', 'final',
             'merge', 'real2', 'real1', 'near', 'after', 'ultimate'),
    'hour': ('final', 'merge', 'real', 'near', 'after', 'ultimate'),
    'day': ('final', 'merge', 'real', 'near', 'after', 'ultimate'),
}


def add_nowcast_stores(base):
    # nowcast stores
    depth = 37
    for name in ['nowcast1', 'nowcast2']:
        path = join(config.STORE_DIR, base, name)
        if exists(path):
            continue
        kwargs = {'path': path,
                  'delta': datetime.timedelta(minutes=5)}
        kwargs.update(KWARGS)
        kwargs['origin'] = ORIGINS['5min']
        store = stores.Store.create(**kwargs)
        store.create_storage((depth, 1))
        store.create_storage((depth, depth))
        store.create_aggregation('average', depths=(depth, 1))
        store.create_aggregation('average', depths=(depth, depth))
        store.set_default_aggregation('average')


def command():
    """
    """
    # regular stores
    for group_name in DEPTHS:
        group_path = join(config.STORE_DIR, group_name)
        if not exists(group_path):
            os.mkdir(group_path)

        for store_name in DEPTHS[group_name]:
            store_path = join(group_path, store_name)
            if exists(store_path):
                continue

            kwargs = {
                'path': store_path,
                'delta': DELTAS[group_name],
            }
            kwargs.update(KWARGS)
            kwargs['origin'] = ORIGINS[group_name]
            logger.info('Creating {}'.format(store_path))
            store = stores.Store.create(**kwargs)

            space_depths = (1, 256) if store_name == 'final' else (1, 288)
            store.create_storage(depths=space_depths)
            store.create_aggregation('average', depths=space_depths)
            store.set_default_aggregation('average')

            time_depths = DEPTHS[group_name][store_name]
            store.create_aggregation('average', depths=time_depths)

            if min(time_depths) == 1:
                # leave out the extra storage since the store is shallow enough
                # for simple point requests (for other requests we do add the
                # aggregation above) and loading is faster if there is only one
                # base storage
                continue
            store.create_storage(depths=time_depths)

        if group_name == '5min':
            add_nowcast_stores(group_path)

        # group file, for use by store script
        logger.info('Update config for {}.'.format(group_name))
        store_confs = []
        for store_name in ORDERING[group_name]:
            store_path = join(group_name, store_name)
            store_confs.append({'Store': {'path': store_path}})
        conf = {'Group': store_confs}
        conf_path = '{}.json'.format(group_path)
        json.dump(conf, open(conf_path, 'w'), indent=2)

    logger.info('Init procedure completed.')


def get_parser():
    """ Return argument parser. """
    parser = argparse.ArgumentParser(
        description=__doc__
    )
    return parser


def main():
    """ Call command with args from parser. """
    kwargs = vars(get_parser().parse_args())

    logging.basicConfig(**{'stream': sys.stderr, 'level': logging.INFO})

    try:
        return command(**kwargs)
    except:
        logger.exception('An exception has occurred.')
