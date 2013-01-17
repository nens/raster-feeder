#!/usr/bin/python
# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from radar import config

from openradar import utils
from openradar import gridtools
from openradar import scans

from osgeo import gdal
from PIL import Image

import h5py
import logging
import numpy as np
import os
import shlex
import subprocess


def data_image(masked_array, max_rain=20, threshold=0.0):
    """ """
    basegrid = scans.BASEGRID
    rasterlayerkwargs = utils.rain_kwargs(
        name='jet', max_rain=max_rain, threshold=threshold,
    )
    return gridtools.RasterLayer(
        array=masked_array,
        extent=basegrid.extent,
        projection=basegrid.projection,
    **rasterlayerkwargs).image()


def shape_image():
    """ Return rgba image with shape of country. """
    basegrid = scans.BASEGRID
    shape_layer = basegrid.create_vectorlayer()
    shape_layer.add_line(
        os.path.join(config.SHAPE_DIR, 'west_europa_lijn.shp'),
        color='k',
        linewidth=1,
    )
    return shape_layer.image()


def osm_image():
    """ Return rgba image with osm background. """
    ds_osm_rd = gdal.Open(os.path.join(config.SHAPE_DIR, 'osm-rd.tif'))
    osm_rgba = np.ones(
        (ds_osm_rd.RasterYSize, ds_osm_rd.RasterXSize, 4),
        dtype=np.uint8,
    ) * 255
    osm_rgba[:, :, 0:3] = ds_osm_rd.ReadAsArray().transpose(1 ,2, 0)
    return Image.fromarray(osm_rgba)


def white_image():
    """ Return white rgba image. """
    basegrid = scans.BASEGRID
    white_rgba = np.ones(
        basegrid.size + (4,),
        dtype=np.uint8,
    ) * 255
    return Image.fromarray(white_rgba)


def create_geotiff(dt_aggregate, code='5min'):

    pathhelper = utils.PathHelper(
        basedir=config.AGGREGATE_DIR,
        code=code,
        template='{code}_{timestamp}.h5'
    )

    rasterlayerkwargs = utils.rain_kwargs(
        name='jet', max_rain=2, threshold=0.008,
    )

    aggregatepath = pathhelper.path(dt_aggregate)

    tifpath_rd = os.path.join(
        config.IMG_DIR, 'geotiff', 'rd',
        dt_aggregate.strftime('%Y-%m-%d-%H-%M.tiff')
    )
    tifpath_google = os.path.join(
        config.IMG_DIR, 'geotiff', 'google',
        dt_aggregate.strftime('%Y-%m-%d-%H-%M.tiff')
    )

    with h5py.File(aggregatepath, 'r') as h5:
        array = h5['precipitation']
        mask = np.equal(array, h5.attrs['fill_value'])
        masked_array = np.ma.array(array, mask=mask)

        # Create the rd tiff.
        utils.makedir(os.path.dirname(tifpath_rd))
        gridtools.RasterLayer(array=masked_array,
                              extent=h5.attrs['grid_extent'],
                              projection=h5.attrs['grid_projection'],
                              **rasterlayerkwargs).save(tifpath_rd, rgba=True)

    logging.info('saved {}.'.format(tifpath_rd))
