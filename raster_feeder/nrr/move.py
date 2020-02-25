# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
# -*- coding: utf-8 -*-
"""
Move data from one store to another, flushing the store but leaving it intact.
"""

import argparse
import logging
import os
import sys

from raster_store import load

import turn

from . import config

logger = logging.getLogger(__name__)


def get_store(time_name, store_name):
    """ Return a raster store. """
    path = os.path.join(config.STORE_DIR, time_name, store_name)
    return load(path)


def move_target_chunk_equivalent(source, target):
    """
    Move at most an amount of bands equal to the target's max depth from
    source to target.
    """
    # start move at beginning of source's period
    start = source.period[0]

    # for the target, determine first and last bands
    depth = target.max_depth
    position_seconds = (start - target.timeorigin).total_seconds()
    timedelta_seconds = target.timedelta.total_seconds()
    first = int(round(position_seconds / timedelta_seconds))
    last = depth * ((first // depth) + 1) - 1

    # stop at last date in chunk or end of store, whichever comes first
    stop = min(source.period[1], target.timeorigin + target.timedelta * last)

    # let's move
    logger.info('Move between {} and {}.'.format(start, stop))
    target.update(source, start=start, stop=stop)
    source.delete(start=start, stop=stop)


def move(time_name, source_name, target_name):
    """
    Move data from one radar store into another.
    """
    logger.info('Move procedure initiated for {}.'.format(time_name))

    # init target
    target = get_store(time_name=time_name, store_name=target_name)

    # promote
    locker = turn.Locker(
        host=config.REDIS_HOST,
        db=config.REDIS_DB,
        password=config.REDIS_PASSWORD
    )
    label = 'move: {} => {}'.format(source_name, target_name)
    template = "Move from '{}/{}' into '{}/{}'."
    message = template.format(time_name, source_name, time_name, target_name)
    logger.info(message)
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
    kwargs = vars(get_parser().parse_args())

    # logging
    if kwargs.pop('verbose'):
        basic = {'stream': sys.stderr,
                 'level': logging.INFO,
                 'format': '%(message)s'}
    else:
        basic = {'level': logging.INFO,
                 'format': '%(asctime)s %(levelname)s %(message)s',
                 'filename': os.path.join(config.LOG_DIR, 'nrr_move.log')}
    logging.basicConfig(**basic)

    # run
    move(**kwargs)
