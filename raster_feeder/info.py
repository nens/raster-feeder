# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from os.path import basename
import argparse
import logging

from raster_store import load
from dask_geomodeling.raster import Group

logger = logging.getLogger(__name__)


def get_parser():
    """ Return argument parser. """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'path',
        metavar='PATH',
        help='Path to geoblock configuration json file.',
    )
    return parser


def summary(group):
    """ Return summary text for a group of stores. """
    template = '{:10}: {} - {}, size {:6}, chunks {:>3}x{}'
    store_summaries = []
    for source in group.args:
        if source:
            start, stop = source.period
        else:
            start, stop = 2 * (19 * ' ',)
        store = source.store
        store_summary = template.format(
            basename(store.path),
            start,
            stop,
            len(store),
            store.least_chunks,
            store.max_depth,
        )
        store_summaries.append(store_summary)
    return 'Group:\n' + '\n'.join(store_summaries)


def blockinfo(path):
    """ Show store representation. """
    geoblock = load(path, cold=True)
    if isinstance(geoblock, Group):
        print(summary(geoblock))
    else:
        print(geoblock)


def main():
    """ Call blockinfo with args from parser. """
    return blockinfo(**vars(get_parser().parse_args()))
