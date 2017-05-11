# -*- coding: utf-8 -*<F7>-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
This is the global configuration file. It allows for a 'global' localconfig,
too, which should be put in the same directory as this module.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from os.path import abspath, dirname, join

BUILDOUT_DIR = abspath(join(dirname(__file__), '..'))

# log directories
LOG_DIR = join(BUILDOUT_DIR, 'var', 'log')

# where to put the stores
STORE_DIR = join(BUILDOUT_DIR, 'var', 'store')

# redis host for mtime cache and turn locking system
REDIS_HOST = 'localhost'
REDIS_DB = 0

# Import local settings
try:
    from .localconfig import *  # NOQA
except ImportError:
    pass
