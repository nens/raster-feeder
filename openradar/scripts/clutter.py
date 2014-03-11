
#!/usr/bin/python
# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from radar import config
from radar import gridtools
from radar import log
from radar import scans
from radar import utils

from osgeo import gdal

import argparse
import datetime
import h5py
import json
import logging
import numpy as np
import os

RAIN_THRESHOLD = 1000
FORMAT = '%Y-%m-%d %H:%M:%S'


def _get_args():
    parser = argparse.ArgumentParser(
        description='Count clutter on rainless days',
    )
    parser.add_argument(
        'range',
        type=str,
        help='Ranges to use, for example 20110101-20110103,20110105',
    )
    parser.add_argument(
        '-t', '--tempoutput',
        type=str,
        default=os.path.join(config.SHAPE_DIR, 'tempclutter.h5'),
        help='Temporary output filename',
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        default=os.path.join(config.SHAPE_DIR, 'clutter.h5'),
        help='Ouput filename',
    )
    return vars(parser.parse_args())


def _get_single_clutter(pathhelper, compositedatetime):
    """ Return clutter for a single datetime. """
    path = pathhelper.path(compositedatetime)
    logging.debug('Checking clutter in {}'.format(path))

    # Check if all radars present and composite not decluttered
    composite = gdal.Open(path)
    if composite is None:
        logging.warn('No composite found for {}'.format(compositedatetime))
        return None
    metadata = composite.GetMetadata()
    requested_radars = json.loads(metadata['requested_stations'])
    if set(requested_radars) != set(config.ALL_RADARS):
        logging.warn('Composite does not contain all radars.')
        return None

    if (metadata.get('declutter_size') not in [None, '0'] or
        metadata.get('declutter_history') not in [None, 'None']):
        logging.warn('Decluttered composite!')
        return None

    # Decide if this is clutter or rain
    compositesum = gridtools.ds2ma(composite).filled(0).sum()
    if compositesum > RAIN_THRESHOLD:  # Too much rain in composite
        logging.debug('Too much rain: {:.1f}, skipping.'.format(
            compositesum,
        ))
        return None

    single_clutter = {}
    logging.debug('Reading multiscan')
    multiscan_h5 = scans.MultiScan(
        multiscandatetime=compositedatetime,
        scancodes=config.KNMI_RADARS,
    ).get()

    for radar in config.KNMI_RADARS:
        logging.debug('Reading {} from multiscan'.format(radar))
        radar_h5 = multiscan_h5.get(radar)
        if radar_h5 is None:
            return None  # Only if both composites work, return single clutter.
        
        # Count clutter
        rain = radar_h5['rain']
        fill_value = radar_h5['rain'].attrs['fill_value']
        single_clutter[radar] = np.ma.array(
            rain,
            mask = np.equal(rain, fill_value),
        ).filled(0)

    return single_clutter


def clutter():
    """ 
    Gather statistics and write to h5 file in shape dir. Data is
    first written to a tempfile which is not deleted, so that it can be
    extended in multiple runs.

    At the end of every execution the output file is recreated based on
    the current contents of the tempfile.
    """
    log.setup_logging()

    args = _get_args()
    logging.info('Start summing clutter for {}'.format(args['range']))

    pathhelper = utils.PathHelper(
        basedir=config.COMPOSITE_DIR,
        code=config.COMPOSITE_CODE,
        template='{code}_{timestamp}.tif',
    )

    multidaterange = utils.MultiDateRange(args['range'])
    h5path = args['output']
    h5temppath = args['tempoutput']

    # Open h5 and initialize if necessary
    h5temp = h5py.File(h5temppath)
    if not len(h5temp):
        for compositedatetime in multidaterange.iterdatetimes():
            composite = gdal.Open(pathhelper.path(compositedatetime))
            shape = gridtools.ds2ma(composite).shape
            for radar in config.KNMI_RADARS:
                h5temp.create_dataset(
                    radar,
                    (0, ) + shape,
                    dtype='f4',
                    compression='gzip',
                    shuffle=True,
                    maxshape=(None, ) + shape
                )
            h5temp.create_dataset(
                'count',
                (0, ),
                dtype='i1',
                maxshape=(None, ),
            )
            h5temp.create_dataset(
                'timestamp',
                (0, ),
                dtype='S19',
                maxshape=(None, ),
            )
            break

    timestamp_size = h5temp['timestamp'].shape[0]
    if timestamp_size:
        last_in_file = datetime.datetime.strptime(
            h5temp['timestamp'][-1], FORMAT,
        )
    else:
        last_in_file = None

    position_clutter = h5temp['NL60'].shape[0]
    position_all = h5temp['timestamp'].shape[0]

    for compositedatetime in multidaterange.iterdatetimes():
        # Skip if we already have this in h5temp
        if last_in_file and compositedatetime <= last_in_file:
            continue

        try:
            single_clutter = _get_single_clutter(pathhelper, compositedatetime)
        except Exception as error:
            logging.error('An error occured: {}'.format(error))
            single_clutter = None

        h5temp['count'].resize(position_all + 1, axis=0)

        if single_clutter is None:
            h5temp['count'][position_all] = 0
        else:
            h5temp['count'][position_all] = 1
            for radar in config.KNMI_RADARS:
                h5temp[radar].resize(position_clutter + 1, axis=0)
                h5temp[radar][position_clutter] = single_clutter[radar]
            position_clutter += 1

        h5temp['timestamp'].resize(position_all + 1, axis=0)
        h5temp['timestamp'][position_all] = compositedatetime.strftime(FORMAT)
        position_all += 1

        
    cluttercount = h5temp['count'][:].sum()
    if cluttercount:
        # Write clutter h5 file
        h5 = h5py.File(h5path, 'w')

        for radar in config.KNMI_RADARS:

            # Calculate sum of this radar in tempfile
            h5tempradarsum = np.zeros(h5temp[radar][0].shape)
            for h5tempradar in h5temp[radar]:
                h5tempradarsum += h5tempradar

            # Write to result
            h5.create_dataset(
                radar,
                h5temp[radar].shape[1:],
                dtype='f4',
                compression='gzip',
                shuffle=True,
            )
            h5[radar][:] = h5tempradarsum
        h5.attrs['cluttercount'] = cluttercount
        h5.attrs['range'] = b'{} - {}'.format(
            h5temp['timestamp'][0],
            h5temp['timestamp'][-1],
        )

        h5.close()
    h5temp.close()
    logging.info('Done summing clutter.')
