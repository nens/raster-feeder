# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Create the necessary stores if they do not yet exist and update the group
wrapper configuration.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from datetime import datetime as Datetime
from datetime import timedelta as Timedelta
from os.path import join
import argparse

from . import config
from ..common import create_tumbler


def init_nowcast():
    """ Create HARMONIE stores for configured parameters. """
    create_tumbler(
        path=join(config.STORE_DIR, config.NAME),
        depth=config.DEPTH,
        average=True,
        dtype='f4',
        delta=Timedelta(**config.DELTA),
        projection=config.PROJECTION,
        geo_transform=config.GEO_TRANSFORM,
        origin=Datetime(**config.ORIGIN),
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
    init_nowcast()
