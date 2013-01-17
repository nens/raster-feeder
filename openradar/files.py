
# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from radar import config

from openradar import scans

import datetime
import logging
import os
import shutil
import time
import zipfile


def move_to_zip(source_path, target_path):
    """ Move the file at source_path to a zipfile at target_path. """
    # Prepare
    root, ext = os.path.splitext(target_path)
    zip_path = root + '.zip'
    arcname = os.path.basename(target_path)

    # Write to zip
    with zipfile.ZipFile(zip_path, 'w',
                      compression=zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.write(source_path, arcname=arcname)
    
    # Remove source
    os.remove(source_path)


def organize_from_path(sourcepath):
    """ Walk basepath and move every scan in there to it's desired location """
    logging.info('Starting organize from {}'.format(
        sourcepath,
    ))
    count = 0

    for path, dirs, names in os.walk(sourcepath):
        for name in names:

            # Is it radar?
            try:
                scan_signature = scans.ScanSignature(scanname=name)
            except ValueError:
                scan_signature = None

            # Is it ground?
            try:
                ground_data = scans.GroundData(dataname=name)
            except ValueError:
                ground_data = None

            if scan_signature:
                target_path = scan_signature.get_scanpath()
            elif ground_data:
                target_path = ground_data.get_datapath()
            else:
                logging.debug(
                    'Could not determine target path for {}'.format(name),
                )
                continue

            source_path = os.path.join(path, name)
            if not os.path.exists(os.path.dirname(target_path)):
                os.makedirs(os.path.dirname(target_path))
            if target_path.endswith('.csv'):
                move_to_zip(source_path, target_path)
            else:
                shutil.move(source_path, target_path)
            count += 1
    logging.info('Moved {} files'.format(count))


def wait_for_files(dt_calculation, td_wait=None, sleep=10):
    """
    Return if files are present or utcnow > dt_files + td_wait

    Waiting for config.ALL_RADARS and ground 5min file.
    """
    if td_wait is None:
        td_wait = config.WAIT_EXPIRE_DELTA


    logging.info('Waiting for files until {}.'.format(
        dt_calculation + td_wait,
    ))

    dt_radar = dt_calculation - datetime.timedelta(minutes=5)
    dt_ground = dt_calculation

    set_expected = set()

    # Add radars to expected files.
    for radar in config.ALL_RADARS:
        set_expected.add(scans.ScanSignature(
            scancode=radar, scandatetime=dt_radar,
        ).get_scanname())
    set_expected.add(scans.GroundData(
        datacode='5min', datadatetime=dt_ground,
    ).get_dataname())

    logging.debug('looking for {}'.format(', '.join(set_expected)))

    # keep walking the source dir until all
    # files are found or the timeout expires.

    while True:
        set_arrived = set()
        for path, dirs, names in os.walk(config.SOURCE_DIR):
            set_names = set(names)
            set_arrived |= (set_names & set_expected)

        if set_arrived:
            set_expected -= set_arrived
            logging.debug('Found: {}'.format(', '.join(set_arrived)))
            if not set_expected:
                logging.info('All required files have arrived.')
                return True
            logging.debug('Awaiting: {}'.format(
                ', '.join(set_expected),
            ))
        
        if datetime.datetime.utcnow() > dt_calculation + td_wait:
            break
        
        time.sleep(config.WAIT_SLEEP_TIME)

    logging.info('Timeout expired, but {} not found.'.format(
        ', '.join(set_expected),
    ))

    return False
