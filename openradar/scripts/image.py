#!/usr/bin/python
# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from openradar import config
from openradar import images
from openradar import loghelper
from openradar import utils
from openradar import scans
from openradar import products

import argparse


def get_image_args():
    parser = argparse.ArgumentParser(
        description='Create images from composites.',
    )
    parser.add_argument('range',
                        metavar='RANGE',
                        type=str,
                        help='Range for datetimes 20110101-20110103,20110105')
    parser.add_argument('--postfix',
                        type=str,
                        default='',
                        help='Postfix for filename.')
    parser.add_argument('--label',
                        type=str,
                        default='',
                        help='Label text in image.')
    parser.add_argument('-p', '--prodcode',
                        choices=['r', 'n', 'a'],
                        default='r',
                        help='(r)ealtime, (n)ear-realtime or (a)fterwards')
    parser.add_argument('-c', '--product',
                        choices=['a', 'b', 'c', 'n'],
                        default='b',
                        help=('(a)ggregate, cali(b)rated, '
                              '(c)onsistent or (n)owcast'))
    parser.add_argument('-t', '--timeframe',
                        choices=['f', 'h', 'd'],
                        default='f',
                        help='(f)ive minute, (h)our or (d)ay')
    parser.add_argument('-f', '--format',
                        type=str,
                        default='png',
                        choices=['png', 'tif'],
                        help='Save "tif" or "png"')
    return vars(parser.parse_args())


def product_generator(product, prodcode, timeframe, datetimes):
    """ Return product generator. """
    for datetime in datetimes:
        if product == 'a':
            yield scans.Aggregate(
                declutter=None,
                radars=config.ALL_RADARS,
                datetime=datetime,
                timeframe=timeframe,
            )
        if product == 'b':
            yield products.CalibratedProduct(datetime=datetime,
                                             prodcode=prodcode,
                                             timeframe=timeframe)
        if product == 'c':
            yield products.ConsistentProduct(datetime=datetime,
                                             prodcode=prodcode,
                                             timeframe=timeframe)
        
        if product == 'n':
            yield products.NowcastProduct(datetime=datetime,
                                          prodcode=prodcode,
                                          timeframe=timeframe)


def main():
    """ Create images for a range of products. """
    loghelper.setup_logging()

    # Get products according to args
    args = get_image_args()

    if args['format'] != 'png':
        raise NotImplementedError('Only png implemented yet.')

    multidaterange = utils.MultiDateRange(args['range'])
    products = product_generator(product=args['product'],
                                 prodcode=args['prodcode'],
                                 timeframe=args['timeframe'],
                                 datetimes=multidaterange.iterdatetimes())

    # Create images with those products
    kwargs = args.copy()
    map(kwargs.pop, ['range', 'product', 'timeframe', 'prodcode'])
    images.create_png(products, **kwargs)
