#!/usr/bin/python
# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from radar import config

from openradar import log
from openradar import utils
from openradar import files
from openradar import products

import argparse
import datetime
import logging


def master_single_product(prodcode, timeframe, datetime):
    """ Returns the created products. """
    products_created = []
    if timeframe not in utils.timeframes(datetime=datetime):
        # Timframe not compatible with datetime
        return products_created
    logging.info('Creating {prodcode}, {timeframe} for {datetime}'.format(
            prodcode=prodcode, timeframe=timeframe, datetime=datetime,
    ))

    # Calibrated products
    calibrated_product = products.CalibratedProduct(
        product=prodcode,
        timeframe=timeframe,
        date=datetime,
    )
    calibrated_product.make()
    products_created.append(calibrated_product)

    # Consistent products, if possible:
    consistent_products_created = []
    for calibrated_product in products_created:
        consistent_products_created.extend(  
            products.Consistifier.create_consistent_products(calibrated_product)
        )

    # Add to products_created
    products_created += consistent_products_created
    logging.info('Created {} product'.format(len(products_created)))
    return products_created


def master_manual(args):
    """ Manual mode """
    datetimes = utils.MultiDateRange(args['range']).iterdatetimes()
    for datetime in datetimes:
        for prodcode in args['product']:
            for timeframe in args['timeframe']:
                yield dict(
                    timeframe=timeframe, 
                    prodcode=prodcode, 
                    datetime=datetime,
                )
    

def master_auto(args, dt_delivery):
    """
    auto mode; destined to run from cronjob. Makes the products
    that are possible now based on the delivery time.
    """
    delivery_time=dict(
        r=datetime.timedelta(),
        n=datetime.timedelta(hours=1),
        a=datetime.timedelta(days=2),
    )

    for prodcode in args['product']:
        for timeframe in args['timeframe']:
            yield dict(
                timeframe=timeframe, 
                prodcode=prodcode, 
                datetime=dt_delivery - delivery_time[prodcode]
            )


def master():
    log.setup_logging()
    args = master_args()
    logging.info('Master start')
    try:

        if args['range'] is not None:
            jobs = master_manual(args)
        else:
            dt_delivery = utils.closest_time()
            jobs = master_auto(args, dt_delivery)
            files.wait_for_files(dt_calculation=dt_delivery)
        
        # Organize 
        sourcepath = args['source_dir']
        files.organize_from_path(sourcepath=sourcepath)

        # Create products
        products_created = []
        for job in jobs:
            products_created.extend(master_single_product(**job))
        
    except Exception as e:
        logging.error('Exception during product creation.')
        logging.exception(e)

    # Separate handling, to publish products created before eventual crash.
    try:
        # Publish products
        products.publish(products_created)
    except Exception as e:
        logging.error('Exception during publication.')
        logging.exception(e)

    logging.info('Master stop')


def master_args():
    parser = argparse.ArgumentParser(
        description='Get latest <timeframe> image or '
                    ' get date range and process it to a calibrated image'
    )
    parser.add_argument(
        '-p', '--product',
        nargs='*',
        choices=['r', 'n', 'a'],
        default=['r', 'n', 'a'],
        help=('Choose product near-realtime, realtime etc.. Options are \n'
              'r = realtime\n'
              'n = near realtime\n'
              'a = afterwards'))
    parser.add_argument(
        '-t', '--timeframe',
        nargs='*',
        choices=['f', 'h', 'd'],
        default=['f', 'h', 'd'],
        help=('Choose time frame of the data options are: \n'
              'f = five minute (e.g. at 15.55) \n'
              'h = hourly aggregates (on whole hours e.g. 09.00 \n'
              'd = daily aggregates (24 hours from 08.00 AM to 07.55 AM'))
    parser.add_argument(
        '-r', '--range',
        metavar='RANGE',
        type=str,
        help='Ranges to use, for example 20110101-20110103,20110105')
    parser.add_argument(
        '-s', '--source-dir',
        type=str,
        default=config.SOURCE_DIR,
        help=('Path from where all the files'
              ' are stored that need to be organized'),
    )
    return vars(parser.parse_args())
