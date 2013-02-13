#!/usr/bin/python
# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import importlib

from openradar import config


def from_object(path):
    """ 
    Update configuration with attributes defined on object.

    Path can be a dotted path pointing to a module, or a dotted path to
    a class definition.
    """
    try:
        # Config from a module
        config_object = importlib.import_module(path)
    except ImportError:
        # Config from an object in the module
        elements = path.split('.')
        module_path, object_name = '.'.join(elements[:-1]), elements[-1]
        config_module = importlib.import_module(module_path)
        config_object = getattr(config_module, object_name)

    # The actual update
    for k, v in config_object.__dict__.items():
        if not k.startswith('_'):
            setattr(config, k, v)
