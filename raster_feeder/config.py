# -*- coding: utf-8 -*<F7>-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
This is the global configuration file. It allows for a 'global' localconfig,
too, which should be put in the same directory as this module.
"""

import pathlib

# directories
PACKAGE_DIR = pathlib.Path(__file__).parent.parent
STORE_DIR = PACKAGE_DIR / "var" / "store"
LOG_DIR = PACKAGE_DIR / "var" / "log"

# make sure they exist - there must be a better way
STORE_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# redis host for mtime cache
REDIS_HOST = 'localhost'
REDIS_DB = 0
REDIS_PASSWORD = None

# redis host for turn locking system
REDIS_HOST_TURN = 'localhost'

# Lizard API credentials
LIZARD_USERNAME = 'override'
LIZARD_PASSWORD = 'override'
LIZARD_TEMPLATE = 'override'

# sentry
SENTRY_DSN = None  # put in localconfig: 'https://<key>@sentry.io/<project>'

# Import local settings
try:
    from .localconfig import *  # NOQA
except ImportError:
    pass
