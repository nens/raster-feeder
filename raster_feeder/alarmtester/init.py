# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Create the necessary stores if they do not yet exist and update the group
wrapper configuration.
"""

import argparse
import os

from datetime import timedelta as Timedelta

from . import config
from ..common import create_tumbler


def init_alarmtester():
    """ Create alarm tester stores for configured parameters. """
    create_tumbler(
        path=os.path.join(config.STORE_DIR, config.NAME),
        depth=config.DEPTH,
        dtype='f4',
        delta=Timedelta(minutes=5),
        projection=config.PROJECTION,
        geo_transform=config.GEO_TRANSFORM,
    )


def get_parser():
    """ Return argument parser. """
    parser = argparse.ArgumentParser(
        description=__doc__
    )
    return parser


def main():
    """ Call command with args from parser. """
    get_parser().parse_args()
    init_alarmtester()
