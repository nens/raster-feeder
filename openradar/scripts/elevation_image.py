# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
""" TODO Docstring. """

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import argparse
import logging
import sys

import h5py
import numpy as np

from PIL import Image
from matplotlib import colors
from matplotlib import cm

from openradar import calc
from openradar import config

logger = logging.getLogger(__name__)


def get_parser():
    """ Return argument parser. """
    parser = argparse.ArgumentParser(
        description=__doc__
    )
    parser.add_argument(
        'paths',
        nargs='+',
        metavar='FILES',
    )
    return parser


def command(paths):
    """ Show a lowest elevation image. """
    # load
    d_rang, d_elev, d_anth = {}, {}, {}
    for p in paths:
        with h5py.File(p, 'r') as h5:
            for r in h5:
                if r in d_rang:
                    continue
                d_rang[r] = h5[r]['range'][:]
                d_elev[r] = h5[r]['elevation'][:]
                d_anth[r] = h5[r].attrs['antenna_height']
    radars = d_anth.keys()
    elev = np.ma.empty((len(radars),) + d_rang[radars[0]].shape)
    rang = np.ma.empty((len(radars),) + d_rang[radars[0]].shape)
    anth = np.empty((len(radars), 1, 1))
    for i, r in enumerate(radars):
        elev[i] = np.ma.masked_equal(d_elev[r], config.NODATAVALUE)
        rang[i] = np.ma.masked_equal(d_rang[r], config.NODATAVALUE)
        anth[i] = float(d_anth[r]) / 1000

    # calculate
    theta = calc.calculate_theta(
        rang=rang, elev=np.radians(elev), anth=anth,
    )
    alt = calc.calculate_height(
        theta=theta, elev=np.radians(elev), anth=anth,
    )
    which = np.ma.where(
        alt == alt.min(0),
        np.indices(alt.shape)[0],
        -1,
    ).max(0)
    what = alt.min(0)

    # colors?
    hue = cm.hsv(colors.Normalize(vmax=len(radars))(which), bytes=True)
    sat = 1 - colors.LogNorm()(what)[..., np.newaxis]

    hue[..., :3] *= sat
    Image.fromarray(hue).show()
    return 0


def main():
    """ Call command with args from parser. """
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    return command(**vars(get_parser().parse_args()))


if __name__ == '__main__':
    exit(main())
