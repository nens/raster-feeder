# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
# -*- coding: utf-8 -*-
"""
Move data from one store to another, flushing the store but leaving it intact.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import argparse
import logging
import os
import sys

from raster_store import stores
import turn

from openradar import config

logger = logging.getLogger(__name__)


def get_store(time_name, store_name):
    """ Return a raster store. """
    path = os.path.join(config.STORE_DIR, time_name, store_name)
    return stores.get(path)


def move_target_chunk_equivalent(source, target):
    """
    Move at most an amount of bands equal to the target's max depth from
    source to target.
    """
    # find the target chunk in which the start of the period of the source is
    start = source.period[0]
    depth = target.max_depth
    first = target.select_bands(start)[0]
    last = depth * ((first // depth) + 1)
    stop = target.get_time_for_bands((last - 1, last))[0]

    # let's move
    logger.debug('Move data between {} and {}'.format(start, stop))
    target.update(source, start=start, stop=stop)
    source.delete(start=start, stop=stop)


def move(time_name, source_name, target_name, verbose):
    """
    Move data from one radar store into another.
    """
    # logging
    if verbose:
        kwargs = {'stream': sys.stderr,
                  'level': logging.INFO}
    else:
        kwargs = {'level': logging.INFO,
                  'format': '%(asctime)s %(levelname)s %(message)s',
                  'filename': os.path.join(config.LOG_DIR, 'move.log')}
    logging.basicConfig(**kwargs)
    logger.info('Move procedure initiated for {}.'.format(time_name))

    # init target
    target = get_store(time_name=time_name, store_name=target_name)

    # promote
    locker = turn.Locker()
    label = 'move: {} => {}'.format(source_name, target_name)
    logger.info('Move from {} into {}'.format(source_name, target_name))
    source = get_store(time_name=time_name, store_name=source_name)
    while source:
        # lock in chunks to let other processes do things, as well.
        with locker.lock(resource=time_name, label=label):
            move_target_chunk_equivalent(source=source, target=target)
    logger.info('Move procedure completed.')


def get_parser():
    """ Return argument parser. """
    parser = argparse.ArgumentParser(
        description=__doc__
    )
    parser.add_argument(
        'time_name',
        metavar='NAME',
        choices=('5min', 'hour', 'day'),
        help='Timeframe to execute promotion for; used to locking, too.',
    )
    parser.add_argument(
        'source_name',
        metavar='SOURCE',
        help='Source store from which to move data.',
    )
    parser.add_argument(
        'target_name',
        metavar='TARGET',
        help='Target store to move data into.',
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
    )
    return parser


def main():
    """ Call move with args from parser. """
    try:
        move(**vars(get_parser().parse_args()))
        return 0
    except:
        logger.exception('An execption occurred:')
        return 1
