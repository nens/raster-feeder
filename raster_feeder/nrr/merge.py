# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
# -*- coding: utf-8 -*-
"""
Merge realtime, near-realtime, after and ultimate stores into a single
'merge' store.
"""

import argparse
import logging
import os
import sys

from . import config
from . import move

logger = logging.getLogger(__name__)


def merge():
    """ Call move for a number of stores. """
    logger.info('Merge procedure initiated.')

    source_names = {
        'day': ('real', 'near', 'after', 'ultimate'),
        'hour': ('real', 'near', 'after', 'ultimate'),
        '5min': ('real2', 'near', 'after', 'ultimate'),
    }

    for time_name in ('day', 'hour', '5min'):
        for source_name in source_names[time_name]:
            move.move(target_name='merge',
                      time_name=time_name,
                      source_name=source_name)

    logger.info('Merge procedure completed.')


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
    """ Call merge with args from parser. """
    kwargs = vars(get_parser().parse_args())

    # logging
    if kwargs.pop('verbose'):
        basic = {'stream': sys.stderr,
                 'level': logging.INFO,
                 'format': '%(message)s'}
    else:
        basic = {'level': logging.INFO,
                 'format': '%(asctime)s %(levelname)s %(message)s',
                 'filename': os.path.join(config.LOG_DIR, 'nrr_merge.log')}
    logging.basicConfig(**basic)

    # run
    merge(**kwargs)
