# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from osgeo import gdal
from osgeo import osr
import redis
import sentry_sdk

from raster_store import cache

from . import config

# crash on gdal exceptions
gdal.UseExceptions()
osr.UseExceptions()

# use one cache client for all raster store operations
cache.client = redis.Redis(host=config.REDIS_HOST, db=config.REDIS_DB)

# sentry
if config.SENTRY_DSN:
    sentry_sdk.init(config.SENTRY_DSN)
