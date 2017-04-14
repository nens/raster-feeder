# -*- coding: utf-8 -*<F7>-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from ..config import BUILDOUT_DIR  # NOQA
from ..config import STORE_DIR     # NOQA
from ..config import LOG_DIR       # NOQA

# Default nodatavalue
NODATAVALUE = -9999

# Store and group names
GROUP_NAME = 'harmonie'
STORE_NAMES = 'harmonie1', 'harmonie2'

# Geographical orientation
GEO_TRANSFORM = -0.0185, 0.037, 0, 55.8885, 0, -0.023
PROJECTION = 'EPSG:4326'

# redis host for mtime cache and turn locking system - relocate to parent
REDIS_HOST = 'localhost'
REDIS_DB = 0

# remote file name strftime() format
FORMAT = 'harm36_v1_ned_surface_%Y%m%d%H.tgz'

# Import local settings
try:
    from .localconfig import *  # NOQA
except ImportError:
    pass
