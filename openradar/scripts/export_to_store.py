#!/usr/bin/python
# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

"""
This script calls console scripts from openradar and from raster_store
in order to export data from legacy radar datafiles into the raster store.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import argparse
import datetime
import logging
import os
import shutil
import subprocess
import sys
import tempfile

from openradar import config

logger = logging.getLogger(__name__)


def get_parser():
    """ Return argument parser. """
    parser = argparse.ArgumentParser(
        description=""
    )
    parser.add_argument('store')
    parser.add_argument('-d', '--date')
    return parser


def command(store, date=None):
    """ Call the subprocesses and do cleanup. """
    tempdir = tempfile.mkdtemp()

    # the export
    if date:
        datepart = date
    else:
        datepart = datetime.date.today().strftime('%Y%m%d')
    text = datepart + '0000-' + datepart + '2355'

    args = [
        os.path.join(config.BUILDOUT_DIR, 'bin', 'image'),
        text,
        '--product', 'b',
        '--format', 'tif',
        '--image-dir', tempdir,
    ]
    subprocess.call(args)

    # the loading
    args = [
        os.path.join(config.BUILDOUT_DIR, 'bin', 'radar_to_store'),
        store,
    ] + [os.path.join(tempdir, name) for name in sorted(os.listdir(tempdir))]
    subprocess.call(args)

    # cleanup
    shutil.rmtree(tempdir)

    'bin/radar_to_store /media/arjan/Elements/stores/radar/ ~/radarimgs/*tif'


def main():
    """ Call command with args from parser. """
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    command(**vars(get_parser().parse_args()))
