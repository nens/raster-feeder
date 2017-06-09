# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Create the necessary stores if they do not yet exist and update the group
wrapper configuration.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from datetime import datetime as Datetime
from datetime import timedelta as Timedelta
from os.path import join, exists

import argparse
import json
import os

from raster_store import stores

from . import config

# common kwargs
KWARGS = {
    'dtype': 'f4',
    'delta': Timedelta(hours=1),
    'projection': config.PROJECTION,
    'geo_transform': config.GEO_TRANSFORM,
    'origin': Datetime(year=2000, month=1, day=1),
}


def init_harmonie_group(name, depth):
    """
    Create a group with stores.
    """
    group_path = join(config.STORE_DIR, name)
    try:
        os.mkdir(group_path)
    except OSError:
        pass

    store_confs = []
    store_names = name + '1', name + '2'
    for store_name in store_names:
        # append store conf entry
        store_rel_path = join(name, store_name)
        store_confs.append({'Store': {'path': store_rel_path}})

        store_path = join(group_path, store_name)
        if exists(store_path):
            continue

        print('Create store for %s.' % store_rel_path)

        # determine kwargs
        kwargs = {'path': store_path}
        kwargs.update(KWARGS)

        # create the store
        store = stores.Store.create(**kwargs)

        # create storages
        store.create_storage((depth, 1))
        store.create_storage((depth, depth))

    # group file, for use by store script
    print('Update config for %s.' % name)
    group_conf = {'Group': store_confs}
    conf_path = group_path + '.json'
    json.dump(group_conf, open(conf_path, 'w'), indent=2)


def init_harmonie():
    """ Create HARMONIE stores for configured parameters. """
    for parameter in config.PARAMETERS:
        depth = parameter['steps']
        name = parameter['group']
        init_harmonie_group(name=name, depth=depth)
    print('Init procedure completed.')


def get_parser():
    """ Return argument parser. """
    parser = argparse.ArgumentParser(
        description=__doc__
    )
    return parser


def main():
    """ Call command with args from parser. """
    get_parser().parse_args()
    init_harmonie()
