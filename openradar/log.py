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

CONSOLE_LEVEL = 'DEBUG' if config.DEBUG else 'INFO'
LOGFILE = os.path.join(config.LOG_DIR, 'radar.log')

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
            'level': CONSOLE_LEVEL,
            'formatter': 'simple',
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'INFO',
            'formatter': 'verbose',
            'filename': LOGFILE,
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

    if logfile is None:
        logging_dict = LOGGING
    else:
        logging_dict = copy.deepcopy(LOGGING)
        logging_dict['handlers']['file']['filename'] = logfile
    
    try:
        os.makedirs(os.path.dirname(
            logging_dict['handlers']['file']['filename'],
        ))
    except OSError:
        pass  # Already exists
    
    logging.config.dictConfig(logging_dict)
