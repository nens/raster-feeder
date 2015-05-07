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

from openradar import config
from openradar.atomic import move

logger = logging.getLogger(__name__)


def command(verbose):
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
                  'filename': os.path.join(config.LOG_DIR, 'atomic_merge.log')}
    logging.basicConfig(**kwargs)

    logger.info('Merge procedure initiated.')

    source_names = {
        'day': ('real', 'near', 'after'),
        'hour': ('real', 'near', 'after'),
        '5min': ('real2', 'near', 'after'),
    }

    for time_name in ('day', 'hour', '5min'):
        for source_name in source_names[time_name]:
            move.move(verbose=verbose,
                      target_name='merge',
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
    """ Call command with args from parser. """
    return command(**vars(get_parser().parse_args()))
    try:
        command(**vars(get_parser().parse_args()))
        return 0
    except:
        logger.exception('An execption occurred:')
        return 1
