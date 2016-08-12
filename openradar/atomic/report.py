# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Report on the actual state of the radar stores.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import argparse
import datetime
import json
import logging
import os
import smtplib
import sys

from email.mime.text import MIMEText

import redis

from raster_store import stores

from openradar import config
from openradar import periods
from openradar import utils

logger = logging.getLogger(__name__)

# mtime caching
stores.cache = redis.Redis(host=config.REDIS_HOST, db=config.REDIS_DB)

TOLERANCE = datetime.timedelta(minutes=15)
NAMES = {'f': '5min', 'h': 'hour', 'd': 'day'}
HOUR = datetime.timedelta(hours=1)

# calibrations:
KED = 'Kriging External Drift'
IDW = 'Inverse Distance Weighting'
RAW = 'None'

TEMPLATE_LOG = '{datetime} {timeframe} {message}'
TEMPLATE_EMAIL = """
The automated quality checker on the NRR data storage found that some
products were not delivered in time. The following problems were reported:

{}""".strip()


def get_metas(name, period):
    store = stores.get(os.path.join(config.STORE_DIR, name))
    start = period.start.isoformat()
    stop = period.stop.isoformat()
    metas = store.get_meta(start=start, stop=stop)
    return {k: json.loads(v) for k, v in metas.items() if v}


def send_mail(report):
    """ Send report as email. """
    recipients = config.REPORT_RECIPIENTS
    if not recipients:
        logger.info('No recipients configured for report email.')
        return
    sender = config.REPORT_SENDER

    # prepare message
    msg = MIMEText(report)
    msg['Subject'] = '[NRR] Status issue.'
    msg['From'] = sender
    msg['To'] = ','.join(config.REPORT_RECIPIENTS)

    # send
    smtp = smtplib.SMTP(config.REPORT_SMTP_HOST)
    smtp.sendmail(sender, recipients, msg.as_string())
    smtp.quit()


class Checker(object):

    template0 = 'Need {exp} or better, found no product.'
    template1 = 'Need {exp} or better, found {act}.'
    names = {'r': 'realtime', 'n': 'near-realtime', 'a': 'after'}

    def __init__(self, quality):
        # determine zones
        u = datetime.datetime.utcnow() - TOLERANCE
        self.r = utils.closest_time(timeframe='f', dt_close=u)
        self.n = utils.closest_time(timeframe='h', dt_close=u - HOUR)
        self.a = utils.closest_time(timeframe='d', dt_close=u - HOUR * 12)

        self.quality = quality

    def check(self, meta, date):
        # what do we have
        actual_prodcode = meta.get('prodcode', '')
        actual_calibration = meta.get('cal_method', 'None')

        # what do we expect
        if date < self.a:
            expected_prodcode = 'a'
            try:
                composite_count = meta['composite_count']
            except KeyError:
                logger.debug('{}: no product or no meta.'.format(date))
                return
            if composite_count == 1:
                expected_calibration = IDW,
            else:
                expected_calibration = KED,
        elif date < self.n:
            expected_prodcode = 'na'
            expected_calibration = IDW, KED
        elif date < self.r:
            expected_prodcode = 'rna'
            expected_calibration = RAW, IDW, KED
        else:
            return

        # the test
        exp = self.names[expected_prodcode[0]]
        if not actual_prodcode:
            return self.template0.format(exp=exp)
        if actual_prodcode not in expected_prodcode:
            act = self.names[actual_prodcode]
            return self.template1.format(exp=exp, act=act)
        if self.quality and actual_calibration not in expected_calibration:
            act = actual_calibration
            exp = expected_calibration[0]
            return self.template1.format(exp=exp, act=act)


def command(text, verbose, quality):
    """
    """
    # logging
    if verbose:
        kwargs = {'stream': sys.stderr,
                  'level': logging.DEBUG}
    else:
        kwargs = {'level': logging.INFO,
                  'format': '%(asctime)s %(levelname)s %(message)s',
                  'filename': os.path.join(config.LOG_DIR, 'report.log')}
    logging.basicConfig(**kwargs)

    # period
    period = periods.Period(text)
    metas = {t: get_metas(NAMES[t], period) for t in 'fhd'}
    checker = Checker(quality)
    failures = []

    # the checking
    for d in period:
        for t in utils.get_valid_timeframes(d):
            m = checker.check(meta=metas[t].get(d, {}), date=d)
            if m:
                failures.append(TEMPLATE_LOG.format(
                    datetime=d, timeframe=t, message=m,
                ))

    # communicate
    if failures:
        logger.info('There were {} failures'.format(len(failures)))
        for f in failures:
            logger.info(f)
        report = TEMPLATE_EMAIL.format('\n'.join(failures))
        logger.debug('Email body:\n{}'.format(report))
        send_mail(report)
    else:
        logger.debug('Everything o.k.')


def get_parser():
    """ Return argument parser. """
    parser = argparse.ArgumentParser(
        description=__doc__
    )
    parser.add_argument(
        'text',
        metavar='PERIOD',
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
    )
    parser.add_argument(
        '-q', '--quality',
        action='store_true',
        help='Include quality checks.',
    )
    return parser


def main():
    """ Call command with args from parser. """
    try:
        return command(**vars(get_parser().parse_args()))
    except:
        logger.exception('An execption occurred:')
