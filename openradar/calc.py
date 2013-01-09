# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
The Z and R functions should be each others inverse:

    >>> round(Z(R(10.123456)), 3)
    10.123

Rain values must be correct:

    >>> Rain(np.array([56,55,54,7,6]).reshape(-1,1)).get()
    array([[ 99.85188151],
           [ 99.85188151],
           [ 86.46816701],
           [  0.09985188],
           [  0.1       ]])

"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from scipy.ndimage import measurements

import logging
import numpy as np

RADIUS43 = 8495.  # Effective earth radius in km.


def R(Z):
    return np.power(Z / 200., 0.625)


def Z(R):
    return 200. * np.power(R, 1.6)


def weight(r, rtop, rmax):
    """ Must this be radar range, rather than surface dist? """
    w = np.where(
        r > rtop,
        1 - np.square((r - rtop) / (rmax - rtop)),
        np.square(r / rtop),
    )
    return w


def calculate_theta(rang, elev, anth):
    """
    Return angle <radar, center-of-earth, datapoint> in radians.

    rang: distance from radar to datapoint in km
    elev: angle between beam and horizontal plane in radians
    anth: the antenna height above the earth surface in km
    
    horizon_height: height relative to horizon plane
    horizon_dist: distance along horizon plane
    """
    horizon_dist = rang * np.cos(elev)
    horizon_alt = anth + rang * np.sin(elev)

    return np.arctan(horizon_dist / (horizon_alt + RADIUS43))
    

def calculate_cartesian(theta, azim):
    """ 
    Return (x,y) in km along earth surface.
    
    All angles must be in radians.
    azim: clockwise from north
    theta: angle <radar, center-of-earth, datapoint>
    """
    dist = theta * RADIUS43
    return dist * np.sin(azim), dist * np.cos(azim)
    

def calculate_height(theta, elev, anth):
    """ 
    Return height of datapoint above earth surface.

    All angles must be in radians.
    anth: the antenna height above the earth surface in km
    elev: angle between beam and horizontal plane
    theta: angle <radar, center-of-earth, datapoint>
    alpha: angle <datapoint, radar, center-of-earth>
    beta: angle <center-of-earth, datapoint, radar>

    a and b are the lengths of the triangle sides opposite to angles
    alpha and beta respectively.
    """
    alpha = elev + np.pi / 2
    beta = np.pi - theta - alpha
    b = RADIUS43 + anth
    a = b * np.sin(alpha) / np.sin(beta) # Law of sines used here.

    return a - RADIUS43


class Rain(object):
    """ Do calculations from dBZ to rain """
    def __init__(self, dBZ):
        self._dBZ = dBZ.copy()

    def _clip_hail(self):
        self._dBZ[np.greater(self._dBZ, 55)] = 55

    def _make_Z(self):
        self._Z = np.power(10, self._dBZ / 10)

    def _remove_noise(self):
        self._Z[np.less(self._dBZ, 7)] = 0

    def get(self):
        self._clip_hail()
        self._make_Z()
        self._remove_noise()
        return R(self._Z)


def declutter_by_area(array, area):
    """
    Remove clusters with area less or equal to area.
    """
    
    # Create array
    if isinstance(array, np.ma.MaskedArray):
        nonzero = np.greater(array.filled(fill_value=0), 0.1)
    else:
        nonzero = np.greater(array, 0.1)


    logging.debug('Starting size declutter.')
    labels, count1 = measurements.label(nonzero)
    logging.debug('Found {} clusters.'.format(count1))
    areas = measurements.sum(nonzero, labels, labels)
    index = np.less_equal(areas, area)
    nonzero[index] = False
    labels, count2 = measurements.label(nonzero)
    logging.debug(
        'Removed {} clusters with area <= {}.'.format(
            count1 - count2, area,
        ),   
    )
    
    if isinstance(array, np.ma.MaskedArray):
        array.data[index] = 0
    else:
        array[index] = 0
