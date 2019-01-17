# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Common feeder logic.
"""

from os.path import basename, exists, join
import json
import logging
import os

import requests
import turn
import ftplib
import io
import re

from raster_store import load
from raster_store import stores
from . import config

logger = logging.getLogger(__name__)
locker = turn.Locker(host=config.REDIS_HOST, db=config.REDIS_DB)


def create_tumbler(path, depth, **kwargs):
    """
    Create a group with stores suitable for rotation.

    :param path: path to group to be created
    :param depth: temporal depth of storages
    :param kwargs: store creation keyword arguments

    :type path: str
    :type depth: int
    :type kwargs: dict


    """
    if not exists(path):
        os.mkdir(path)

    name = basename(path)
    paths = []

    for store_name in name + '1', name + '2':
        # append store conf entry
        paths.append(join(path, store_name))

        # skip existing
        store_path = join(path, store_name)
        if not exists(store_path):
            print('Create store "%s".' % store_path)

            # create store
            create_kwargs = {'path': store_path}
            create_kwargs.update(kwargs)
            store = stores.Store.create(**create_kwargs)

            # create storages
            store.create_storage((depth, 1))
            store.create_storage((depth, depth))

            store.create_aggregation('topleft', (depth, 1))

    geoblocks_config = {
        'name': 'endpoint',
        'graph': {
            'endpoint': ['geoblocks.raster.combine.Group', 'store1', 'store2'],
            'store1': ['geoblocks.raster.sources.RasterStoreSource', paths[0]],
            'store2': ['geoblocks.raster.sources.RasterStoreSource', paths[1]],
        }
    }
    geoblocks_config_serialized = json.dumps(geoblocks_config, indent=2)
    print('Geoblocks configuration:\n%s' % geoblocks_config_serialized)


def rotate(path, region, resource, label='rotate'):
    """
    Load region in the the currently empty store, then clear the other one.

    :param path: path to raster-storage group containing two stores.
    :param region: raster_store.regions.Region
    :param resource: Resource to lock
    :param label: Label for locking

    It is assumed that one of the stores is empty and the other is
    not. The resource and region parameters are used to lock a resources
    during the modification process, to prevent write attempts on the
    stores during the procedure.

    Should both stores be in the same state (both containing data or
    both being empty), they will be in the proper state after succesful
    rotation, because data is loaded in one store and the other is
    cleared.
    """
    logger.info('Rotation of %s started.' % resource)

    with locker.lock(resource=resource, label='rotate'):
        # load the stores
        old = load(join(path, basename(path) + '1'))
        new = load(join(path, basename(path) + '2'))

        # swap if new already contains data
        if new:
            old, new = new, old

        # put the region in the new store
        new.update([region])

        # delete the data from the old store
        if old:
            start, stop = old.period
            old.delete(start=start, stop=stop)

    logger.info('Rotation of %s completed.' % resource)


def touch_lizard(raster_uuid):
    """Update the raster store metadata using the Lizard API."""
    url = config.LIZARD_TEMPLATE.format(raster_uuid=raster_uuid)
    headers = {
        'username': config.LIZARD_USERNAME,
        'password': config.LIZARD_PASSWORD,
    }

    resp = requests.post(url, headers=headers)
    short_uuid = raster_uuid.split('-')[0]
    if resp.ok:
        logger.info(
            "Metadata update succeeded for %s: %s",
            short_uuid,
            resp.json(),
        )
    else:
        logger.error(
            "Metadata update failed for %s: %s",
            short_uuid,
            resp.status_code,
        )


class FTPServer(object):
    def __init__(self, host, user=None, password=None, path=None):
        """ Connects and switches to  """
        self.connection = ftplib.FTP(host=host, user=user, passwd=password)
        if path is not None:
            self.connection.cwd(path)

    def listdir(self):
        """ Return file listing of current working directory. """
        return self.connection.nlst()

    def get_latest_match(self, re_pattern):
        match = re.compile(re_pattern).match
        try:
            return sorted(filter(match, self.listdir()))[-1]
        except IndexError:
            return

    def _retrieve_to_stream(self, name, stream):
        """ Write remote file to local path. """
        logger.info('Downloading {} from FTP.'.format(name))
        self.connection.retrbinary('RETR ' + name, stream.write)
        stream.seek(0)
        return stream

    def retrieve_to_path(self, name, path):
        """ Write remote file to local path. """
        with open(path, 'w') as f:
            self._retrieve_to_stream(name, f)

    def retrieve_to_stream(self, name):
        """ Write remote file to memory stream. """
        return self._retrieve_to_stream(name, io.BytesIO())

    def close(self):
        self.connection.quit()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
