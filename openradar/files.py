# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from openradar import config
from openradar import scans
from openradar import utils

from datetime import datetime as Datetime
from datetime import timedelta as Timedelta
from os.path import abspath, dirname, exists, join

import ftplib
import logging
import os
import shutil
import time


def organize_from_path(source_dir):
    """ Walk basepath and move every scan in there to it's desired location """
    logging.info('Starting organize from {}'.format(
        source_dir,
    ))
    count = 0

    for path, dirs, names in os.walk(source_dir):
        for name in names:
            scansource = abspath(join(path, name))
            target_path = scans.ScanSignature(
                scansource=scansource,
            ).get_scanpath()

            if not exists(dirname(target_path)):
                os.makedirs(dirname(target_path))
            shutil.move(scansource, target_path)
            count += 1

    logging.info('Processed %s files', count)


class FtpImporter(object):
    """
    Connect to ftp for radars and fetch any files that are not fetched yet.
    """
    def __init__(self, datetime, max_age=86400):
        """
        Set datetime and empty connection dictionary. Max age is in
        seconds, measured from datetime.
        """
        utils.makedir(config.SOURCE_DIR)

        self.datetime = datetime
        self.max_age = max_age
        self.connections = {}
        # Check what is already there.
        self.arrived = []

    def _connect(self, group):
        """ Create and store connection for group on self. """
        ftp = ftplib.FTP(
            config.FTP_RADARS[group]['host'],
            config.FTP_RADARS[group]['user'],
            config.FTP_RADARS[group]['password'],
        )
        ftp.cwd(config.FTP_RADARS[group]['path'])
        self.connections[group] = ftp
        logging.debug('FTP connection to {} established.'.format(group))

    def _sync(self, group):
        """
        Fetch files that are not older than max_age, and that are not
        yet in config.SOURCE_DIR or in config.RADAR_DIR, and store them
        in SOURCE_DIR.
        """
        ftp = self.connections[group]
        remote = ftp.nlst()
        synced = []
        for name in remote:
            scansource = abspath(join(config.SOURCE_DIR, name))
            try:
                scan_signature = scans.ScanSignature(scansource=scansource)
            except ValueError:
                continue  # It is not a radar file as we know it.

            scandatetime = scan_signature.get_datetime()
            path = scan_signature.get_scanpath()
            age = (self.datetime - scandatetime).total_seconds()
            if name in self.arrived or age > self.max_age or exists(path):
                continue

            # Try to retrieve the file, but remove it when errors occur.
            targetpath = join(config.SOURCE_DIR, name)
            try:
                with open(targetpath, 'wb') as scanfile:
                    ftp.retrbinary('RETR ' + name, scanfile.write)
                synced.append(name)
                logging.debug('Fetched: {}'.format(name))
            except ftplib.all_errors:
                logging.warn('Fetch of {} failed.'.format(name))
                if exists(targetpath):
                    os.remove(targetpath)
        return synced

    def fetch(self):
        """ Create connection if necessary and sync any files. """
        # Update what we already have.
        for path, dirs, names in os.walk(config.SOURCE_DIR):
            self.arrived.extend(names)

        # Walk ftp filelists and sync where necessary
        synced = []
        for group in config.FTP_RADARS:
            try:
                if group not in self.connections:
                    self._connect(group)
                synced.extend(self._sync(group))
            except ftplib.all_errors:
                logging.warn('FTP connection problem for {}'.format(group))
                if group in self.connections:
                    del self.connections[group]
        return synced

    def close(self):
        """ Close open connections. """
        for group in self.connections:
            self.connections[group].quit()
            logging.debug('Quit FTP connection to {}'.format(group))


def sync_and_wait_for_files(dt_calculation, td_wait=None, sleep=10):
    """
    Return if files are present or utcnow > dt_files + td_wait

    Waiting for config.ALL_RADARS.
    """
    if td_wait is None:
        td_wait = config.WAIT_EXPIRE_DELTA

    logging.info('Waiting for files until {}.'.format(
        dt_calculation + td_wait,
    ))

    dt_radar = dt_calculation - Timedelta(minutes=5)

    set_expected = set()

    # Add radars to expected files.
    for radar in config.ALL_RADARS:
        scan_signature = scans.ScanSignature(
            scancode=radar, scandatetime=dt_radar,
        )
        if not exists(scan_signature.get_scanpath()):
            set_expected.add(scan_signature.get_scanname())

    logging.debug('looking for {}'.format(', '.join(set_expected)))

    # keep walking the source dir until all
    # files are found or the timeout expires.
    ftp_importer = FtpImporter(datetime=dt_calculation)
    while True:
        fetched = ftp_importer.fetch()
        if fetched:
            logging.info('Fetched {} files from FTP.'.format(len(fetched)))
        set_arrived = set()
        for path, dirs, names in os.walk(config.SOURCE_DIR):
            set_names = set()
            for name in names:
                set_names.add(scans.ScanSignature(
                    scansource=abspath(join(path, name))
                ).get_scanname())
            # Add the intersection of names and expected to arrived.
            set_arrived |= (set_names & set_expected)

        if set_arrived:
            set_expected -= set_arrived
            logging.debug('Found: {}'.format(', '.join(set_arrived)))
            if not set_expected:
                logging.info('All required files have arrived.')
                ftp_importer.close()
                return True
            logging.debug('Awaiting: {}'.format(
                ', '.join(set_expected),
            ))

        if Datetime.utcnow() > dt_calculation + td_wait:
            break

        try:
            logging.debug('Sleeping...')
            time.sleep(config.WAIT_SLEEP_TIME)
        except KeyboardInterrupt:
            break

    logging.info('Timeout expired, but {} not found.'.format(
        ', '.join(set_expected),
    ))

    ftp_importer.close()
    return False
