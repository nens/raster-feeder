# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import copy
import logging.config
import logging
import os

from openradar import config

LOGGING = {
    'disable_existing_loggers': True,
    'version': 1,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(message)s'
        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': None,
            'formatter': 'simple',
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'INFO',
            'formatter': 'verbose',
            'filename': None,
            'mode': 'a',
            'maxBytes': 10485760,
            'backupCount': 5,
        },
    },
    'loggers': {
        'root': {
            'level': 'DEBUG',
            'handlers': ['console', 'file'],
        },
        '': {
            'level': 'DEBUG',
            'handlers': ['console', 'file'],
        },
    }
}


def setup_logging(logfile=None):
    """ Setup logging according to logfile and settings. """
    logging_dict = copy.deepcopy(LOGGING)
    # Set console level from config
    _console_level = 'DEBUG' if config.DEBUG else 'INFO'
    logging_dict['handlers']['console']['level'] = _console_level
    # Set logfile from config or logfile
    if logfile is None:
        _logfile = os.path.join(config.LOG_DIR, 'radar.log')
    else:
        _logfile = logfile
    logging_dict['handlers']['file']['filename'] = _logfile
    # Create directory if necessary
    try:
        os.makedirs(os.path.dirname(
            logging_dict['handlers']['file']['filename'],
        ))
    except OSError:
        pass  # Already exists
    # Config logging
    logging.config.dictConfig(logging_dict)
