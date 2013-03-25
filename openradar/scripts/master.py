#!/usr/bin/python
# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division


from openradar import arguments
from openradar import config
from openradar import files
from openradar import loghelper
from openradar import tasks
from openradar import utils

import datetime
import logging


def master(**kwargs):
    """ Run the radar production chain for a single datetime. """
    loghelper.setup_logging(logfile_name='radar_master.log')
    logging.info(20 * '-' + ' master ' + 20 * '-')

    # Determine the delivery datetime and if necessary wait for files.
    if kwargs['range'] is not None:
        datetimes = utils.DateRange(kwargs['range']).iterdatetimes()
        for i, datetime_delivery in enumerate(datetimes):
            if i > 0:
                logging.warning('Range of datetimes given. Using the first.')
                break
    else:
        datetime_delivery = utils.closest_time()
        files.sync_and_wait_for_files(dt_calculation=datetime_delivery)

    # Organize
    files.organize_from_path(sourcepath=config.SOURCE_DIR)

    # Product datetime depends on delevery times
    declutter = dict(
        size=config.DECLUTTER_SIZE,
        history=config.DECLUTTER_HISTORY,
    )
    radars = config.ALL_RADARS
    delivery_times = dict(
        r=datetime.timedelta(),
        n=datetime.timedelta(hours=1),
        a=datetime.timedelta(days=2),
    )

    # Submit aggregate tasks.
    for prodcode, timedelta_delivery in delivery_times.items():
        datetime_product = datetime_delivery - timedelta_delivery
        combinations = utils.get_aggregate_combinations(
            datetimes=[datetime_product],
        )
        for combination in combinations:
            aggregate_kwargs = dict(declutter=declutter, radars=radars)
            aggregate_kwargs.update(combination)
            tasks.aggregate.delay(**aggregate_kwargs)

            # Submit calibrate tasks
            calibrate_kwargs = dict(prodcode=prodcode)
            calibrate_kwargs.update(aggregate_kwargs)
            tasks.calibrate.delay(**calibrate_kwargs)

            # Submit rescale tasks
            rescale_kwargs = {k: v
                              for k, v in calibrate_kwargs.items()
                              if k in ['datetime', 'prodcode', 'timeframe']}
            tasks.rescale.delay(**rescale_kwargs)

            # Submit publication tasks
            tasks.publish.delay(
                datetimes=[calibrate_kwargs['datetime']],
                prodcodes=[calibrate_kwargs['prodcode']],
                timeframes=[calibrate_kwargs['timeframe']],
                endpoints=['ftp', 'h5', 'local', 'image', 'h5m'],
                cascade=True,
            )

    # Create task to create animated gif
    tasks.animate.delay(datetime=datetime_delivery)

    logging.info(20 * '-' + ' master complete ' + 20 * '-')


def main():
    argument = arguments.Argument()
    parser = argument.parser(
        ['opt_range'],
        description=('Run the radar production chain for '
                     'now, or for a single moment in time.'),
    )
    master(**vars(parser.parse_args()))
