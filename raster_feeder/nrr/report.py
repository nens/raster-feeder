# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
"""
Report on the actual state of the radar stores.
"""

import argparse
import datetime
import json
import logging
import os
import smtplib
import sys

from email.mime.text import MIMEText

import redis

from raster_store import cache
from raster_store import load
from raster_store.interfaces import GeoInterface

from . import config
from . import periods
from . import utils

logger = logging.getLogger(__name__)

# mtime caching
cache.client = redis.Redis(
    host=config.REDIS_HOST,
    db=config.REDIS_DB,
    password=config.REDIS_PASSWORD
)

TOLERANCE = datetime.timedelta(minutes=30)
NAMES = {'f': '5min', 'h': 'hour', 'd': 'day'}
HOUR = datetime.timedelta(hours=1)
DAY = datetime.timedelta(days=1)

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
    store = GeoInterface(load(os.path.join(config.STORE_DIR, name)))
    start = period.start.isoformat()
    stop = period.stop.isoformat()
    metas = store.get_meta(start=start, stop=stop)
    return {k: json.loads(v) for k, v in metas.items() if v}


def send_mail(report):
    """ Send report as email. """
    recipients = getattr(config, 'REPORT_RECIPIENTS', [])
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
    names = {
        'r': 'realtime',
        'n': 'near-realtime',
        'a': 'after',
        'u': 'ultimate',
    }

    def __init__(self, quality):
        # determine zones
        u = datetime.datetime.utcnow() - TOLERANCE
        self.r = utils.closest_time(timeframe='f', dt_close=u)
        self.n = utils.closest_time(timeframe='h', dt_close=u - HOUR)
        self.a = utils.closest_time(timeframe='d', dt_close=u - HOUR * 12)
        self.u = utils.closest_time(timeframe='d', dt_close=u - DAY * 30)

        self.quality = quality

    def check(self, meta, date):
        # what do we have
        actual_prodcode = meta.get('prodcode', '')
        actual_calibration = meta.get('cal_method', 'None')

        # what do we expect
        if date < self.u:
            expected_prodcode = 'u'
            try:
                composite_count = meta['composite_count']
            except KeyError:
                logger.debug('{}: no product or no meta.'.format(date))
                return
            if composite_count == 1:
                expected_calibration = IDW,
            else:
                expected_calibration = KED,
        elif date < self.a:
            expected_prodcode = 'au'
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
            expected_prodcode = 'nau'
            expected_calibration = IDW, KED
        elif date < self.r:
            expected_prodcode = 'rnau'
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


def report(text, quality):
    """
    """
    # period
    period = periods.Period(text)
    logger.info('Report produre initiated.')
    metas = {t: get_metas(NAMES[t], period) for t in 'fhd'}
    logger.info('Metas retrieved.')
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
        logger.debug('There were {} failures'.format(len(failures)))
        for f in failures:
            logger.debug(f)
        body = TEMPLATE_EMAIL.format('\n'.join(failures))
        send_mail(body)
    else:
        logger.debug('Everything o.k.')
    logger.info('Report procedure completed.')


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
    """ Call report with args from parser. """
    kwargs = vars(get_parser().parse_args())

    # logging
    if kwargs.pop('verbose'):
        basic = {'stream': sys.stderr,
                 'level': logging.DEBUG,
                 'format': '%(message)s'}
    else:
        basic = {'level': logging.INFO,
                 'format': '%(asctime)s %(levelname)s %(message)s',
                 'filename': os.path.join(config.LOG_DIR, 'nrr_report.log')}
    logging.basicConfig(**basic)

    # run
    report(**kwargs)
