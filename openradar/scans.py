# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from radar import config

from openradar import calc
from openradar import io
from openradar import utils
from openradar import gridtools

from scipy import interpolate

import datetime
import h5py
import json
import logging
import multiprocessing
import numpy as np
import os
import re

BAND_RAIN = 1
BAND_RANG = 2
BAND_ELEV = 3

BAND_META = {
    BAND_RAIN: dict(name='rain'),
    BAND_RANG: dict(name='range'),
    BAND_ELEV: dict(name='elevation'),
}


def create_basegrid():
    left, right, top, bottom = config.COMPOSITE_EXTENT
    cellwidth, cellheight = config.COMPOSITE_CELLSIZE
    width, height = right - left, top - bottom

    extent = [left, right, top, bottom]
    size = int(width / cellwidth), int(height / cellheight)
    projection = utils.projection(utils.RD)

    return gridtools.BaseGrid(
        extent=extent,
        size=size,
        projection=projection,
    )

BASEGRID = create_basegrid()


class GroundData():
    """
    Hold datetime and code for groundstationfile

    Import from filename or from code and datetime.
    Get filepath.

    TODO:
        - Unify interface with that of scansignature
        - Better name... This is not data.
    """
    def __init__(self, dataname=None, datacode=None, datadatetime=None):
        if datacode and datadatetime and (dataname is None):
            self._code = datacode
            self._datetime = datadatetime
        elif dataname and (datacode is None) and (datadatetime is None):
            self._from_dataname(dataname)
        else:
            raise ValueError(
                'Specify either dataname or datadatetime and datacode.'
            )

    def _from_dataname(self, dataname):
        datacode = self._code_from_dataname(dataname)
        datetime_format = config.TEMPLATE_GROUND.format(code=datacode)
        datadatetime = datetime.datetime.strptime(dataname, datetime_format)

        self._code = datacode
        self._datetime = datadatetime

    def _code_from_dataname(self, dataname):
        match = re.match(config.GROUND_PATTERN, dataname)
        if match:
            return match.group('code')
        raise ValueError(
            "Currently no ground pattern matching '{}'".format(dataname),
        )

    def _format_for_code(self, code):
        return config.TEMPLATE_GROUND.format(code=code)
        raise ValueError(
            "Currently no ground format matching '{}'".format(code),
        )

    def get_dataname(self):
        return datetime.datetime.strftime(
            self._datetime, config.TEMPLATE_GROUND.format(code=self._code),
        )

    def get_datapath(self):
        return os.path.join(
            config.GROUND_DIR,
            self._code,
            self._datetime.strftime('%Y'),
            self._datetime.strftime('%m'),
            self._datetime.strftime('%d'),
            self.get_dataname(),
        )


class ScanSignature(object):
    """
    Hold datetime and code for a scan.

    Import from scanname or from code and datetime.
    Export to scanname or to code and datetime.
    Get timestamp or scanpath.
    """

    def __init__(self, scanname=None, scancode=None, scandatetime=None):
        if scancode and scandatetime and (scanname is None):
            self._datetime = scandatetime

            # Before 2012 DWD radar 'ess' was called 'ase'
            if self._datetime.year < 2012 and scancode == 'ess':
                self._code = 'ase'
            else:
                self._code = scancode

            # After 2012 DWD files have an additional id.
            if self._datetime.year >= 2012:
                self._id = config.RADAR_ID.get(self._code, '')
            else:
                self._id = ''

        elif scanname and (scancode is None) and (scandatetime is None):
            self._from_scanname(scanname)
        else:
            raise ValueError(
                'Specify either scanname or scandatetime and scancode.'
            )

    def _from_scanname(self, scanname):
        radar_dict = self._radar_dict_from_scanname(scanname)
        datetime_format = self._get_datetime_format(radar_dict)
        scandatetime = datetime.datetime.strptime(scanname, datetime_format)

        self._datetime = scandatetime
        self._code = radar_dict['code']
        self._id = radar_dict['id']

    def _radar_dict_from_scanname(self, scanname):
        for pattern in config.RADAR_PATTERNS:
            match = re.match(pattern, scanname)
            if match:
                radar_code = match.group('code')
                try:
                    radar_id = match.group('id')
                except:
                    radar_id = ''
                return {'id': radar_id, 'code': radar_code}
        raise ValueError("Currently no pattern matching '{}'".format(scanname))

    def _get_datetime_format(self, radar_dict):
        """
        Return the datetime format string that corresponds to current scan.
        """
        radar_code = radar_dict['code']
        radar_id = radar_dict['id']

        if radar_code in config.KNMI_RADARS:
            return config.TEMPLATE_KNMI.format(**radar_dict)
        elif radar_code in config.DWD_RADARS:
            if radar_id:
                return config.TEMPLATE_DWD.format(**radar_dict)
            return config.TEMPLATE_DWD_2011.format(**radar_dict)
        elif radar_code in config.DWD_RADARS_2011:
            return config.TEMPLATE_DWD_2011.format(**radar_dict)
        raise ValueError("There is no format for {}".format(radar_dict))

    def get_scanname(self):
        return datetime.datetime.strftime(
            self._datetime, self._get_datetime_format(
                radar_dict={
                    'code': self._code, 'id': self._id,
                },
            ),
        )

    def get_scanpath(self):
        return os.path.join(
            config.RADAR_DIR,
            self._code,
            self._datetime.strftime('%Y'),
            self._datetime.strftime('%m'),
            self._datetime.strftime('%d'),
            self.get_scanname(),
        )

    def get_datetime(self):
        return self._datetime

    def get_timestamp(self):
        return self._datetime.strftime(config.TIMESTAMP_FORMAT)

    def get_code(self):
        if self._code == 'ase':
            return 'ess'
        return self._code

    def get_scan(self):
        if not os.path.exists(self.get_scanpath()):
            logging.warn(
                '{} not found.'.format(self.get_scanpath()),
            )
            return None
        elif self._code in config.DWD_RADARS:
            return ScanDWD(self)
        elif self._code in config.KNMI_RADARS:
            return ScanKNMI(self)
        elif self._code in config.DWD_RADARS_2011:
            return ScanDWD(self)
        logging.error(
            "Currently no scan class matching '{}'".format(self._code),
        )
        return None

    def __repr__(self):
        return '<ScanSignature: {} [{}]>'.format(self._code, self._datetime)

    def __hash__(self):
        """ Bitwise or is a good method for combining hashes. """
        return hash(self._datetime) ^ hash(self._code)


class GenericScan(object):
    """
    Procedures for manipulating scan data.
    """
    def __init__(self, scansignature):
        self.signature = scansignature

    def _interpolate(self, points, values, grid):
        """
        Return numpy masked array.
        """
        # Interpolate
        data_out = interpolate.griddata(
            np.array([point.flatten() for point in points]).transpose(),
            np.array([value.flatten() for value in values]).transpose(),
            grid,
            method='linear',
            fill_value=config.NODATAVALUE,
        ).transpose(2, 0, 1)

        # Create masked array
        ma_out = np.ma.array(
            data_out,
            mask=np.equal(data_out, config.NODATAVALUE),
            fill_value=config.NODATAVALUE,
        )

        return ma_out

    def get(self):
        try:
            data = self.data()
        except IOError as error:
            logging.error('{}: {}'.format(error.strerror, error.filename))
            return None
        rang, azim, elev = data['polar']
        rain = data['rain']
        latlon = data['latlon']
        anth = data['ant_alt']
        # vvv For mocking JABBEKE. Can be removed when JABBEKE is really
        # vvv in production.
        #if self.signature.get_code() == 'JABBEKE':
        #    latlon = (51.179558, 3.09363)
        #    print('adjusted location for JABBEKE')

        theta = calc.calculate_theta(
            rang=rang,
            elev=np.radians(elev),
            anth=anth / 1000,
        )

        points_aeqd = calc.calculate_cartesian(
            theta=theta,
            azim=np.radians(azim),
        )

        projections = (
            utils.projection_aeqd(*latlon),
            utils.projection(utils.RD),
        )

        points_rd = utils.coordinate_transformer.transform(
            points=points_aeqd,
            projections=projections,
        )

        # Interpolate
        grid_rain, grid_rang, grid_elev = self._interpolate(
            points=points_rd,
            values=(
                rain,
                rang * np.ones(rain.shape),
                elev * np.ones(rain.shape),
            ),
            grid=BASEGRID.get_grid(),
        )

        location = utils.transform((0, 0), projections)

        ds_rd = BASEGRID.create_dataset(bands=3)
        ds_rd.SetMetadata(dict(
            source=self.signature.get_scanname(),
            timestamp=self.signature.get_timestamp(),
            station=self.signature.get_code(),
            location=json.dumps(location),
            antenna_height=json.dumps(anth),
            max_elevation=json.dumps(elev.max()),
            min_elevation=json.dumps(elev.min()),
            max_range=json.dumps(rang.max()),
        ))

        banddata = {
            BAND_RAIN: grid_rain,
            BAND_RANG: grid_rang,
            BAND_ELEV: grid_elev,
        }

        for i in range(1, ds_rd.RasterCount + 1):
            band = ds_rd.GetRasterBand(i)
            band.SetNoDataValue(banddata[i].fill_value)
            band.WriteArray(banddata[i].filled())
            band.SetMetadata(BAND_META[i])

        return ds_rd


class ScanKNMI(GenericScan):

    def data(self):
        """ Return data dict for further processing. """
        dataset = h5py.File(self.signature.get_scanpath(), 'r')
        scan_key = 'scan{}'.format(config.KNMI_SCAN_NUMBER)

        latlon = self._latlon(dataset)
        rain = self._rain(dataset[scan_key])
        polar = self._polar(dataset[scan_key])
        ant_alt = config.ANTENNA_HEIGHT[self.signature.get_code()]

        dataset.close()

        return dict(
            latlon=latlon,
            rain=rain,
            polar=polar,
            ant_alt=ant_alt,
        )

    def _latlon(self, dataset):
        """ Return (lat, lon) in WGS84. """
        lon, lat = dataset['radar1'].attrs['radar_location']
        return float(lat), float(lon)

    def _rain(self, dataset):
        """ Convert and return rain values. """
        type_key = 'scan_{}_data'.format(config.KNMI_SCAN_TYPE)
        PV = np.float64(dataset[type_key])
        
        # Get calibration from attribute
        calibration_formula = (
            dataset['calibration'].attrs['calibration_Z_formulas'][0]
        )
        calibration_formula_match = re.match(
            config.CALIBRATION_PATTERN, calibration_formula
        )
        a = float(calibration_formula_match.group('a'))
        b = float(calibration_formula_match.group('b'))
        logging.debug('From "{}": a={}, b={} in dBZ = a * PV + b'.format(
            calibration_formula, a, b,
        ))
            
        dBZ = a * PV + b
        return calc.Rain(dBZ).get()

    def _polar(self, dataset):
        """ Return (rang, azim, elev). """

        range_size = dataset.attrs['scan_number_range']
        range_bin = dataset.attrs['scan_range_bin']
        rang = np.arange(
            0.5 * range_bin,
            (range_size + 0.5) * range_bin,
            range_bin,
        ).reshape(1, -1)

        azim_step = dataset.attrs['scan_azim_bin']
        azim = np.arange(
            azim_step / 2,
            360 + azim_step / 2,
            azim_step,
        ).reshape(-1, 1)

        elev = dataset.attrs['scan_elevation'] * np.ones(azim.shape)
        return rang, azim, elev


class ScanDWD(GenericScan):

    def data(self):
        """ Return data dict for further processing. """
        dBZ, meta = io.readDX(self.signature.get_scanpath())

        return dict(
            latlon=self._latlon(),
            rain=self._rain(dBZ),
            polar=self._polar(meta),
            ant_alt=config.ANTENNA_HEIGHT[self.signature.get_code()]
        )

    def _polar(self, meta):
        rang = np.arange(meta['clutter'].shape[1]).reshape(1, -1) + 0.5
        azim = meta['azim'].reshape(-1, 1) + 180 / meta['azim'].size
        elev = meta['elev'].reshape(-1, 1)
        return rang, azim, elev

    def _rain(self, dBZ):
        """ wradlib did conversion to dBZ. """
        return calc.Rain(dBZ).get()

    def _latlon(self):
        latlon = config.DWD_COORDINATES[self.signature.get_code()]
        return latlon


class MultiScan(object):

    def __init__(self, multiscandatetime, scancodes):
        self.scancodes = scancodes
        self.multiscandatetime = multiscandatetime

        self.path = self.pathhelper = utils.PathHelper(
            basedir=config.MULTISCAN_DIR,
            code=config.MULTISCAN_CODE,
            template='{code}_{timestamp}.h5'
        ).path(multiscandatetime)

    def _add(self, dataset, scan):
        """
        Add scan to dataset, if it is available
        """
        source = scan.get()

        radar = scan.signature.get_code()
        target = dataset.create_group(radar)

        # Add general metadata
        target.attrs['projection'] = source.GetProjection()
        target.attrs['geotransform'] = source.GetGeoTransform()
        for k, v in source.GetMetadata().items():
            target.attrs[k] = v

        # Add bands with their metadata
        for source_band in [source.GetRasterBand(i + 1)
                            for i in range(source.RasterCount)]:
            # Data
            data = source_band.ReadAsArray()
            target_band = target.create_dataset(
                source_band.GetMetadataItem(b'name'),
                data.shape,
                dtype='f4',
                compression='gzip',
                shuffle=True,
            )
            target_band[:] = data

            # Metadata
            target_band.attrs['fill_value'] = source_band.GetNoDataValue()
            for k, v in source_band.GetMetadata().items():
                if k == 'name':
                    continue  # This will be used as the name of the dataset.
                target_band.attrs[k] = v

        logging.debug('{} added to multiscan.'.format(radar))

    def get(self):
        """
        Return a readonly hdf5 dataset for requested scans.

        If the dataset is not available, it is created.
        If it is available, but lacks some scan requested, it is appended.
        """
        utils.makedir(os.path.dirname(self.path))

        try:
            # Improperly closed h5 files cannot be opened.
            dataset = h5py.File(self.path, 'a')
        except IOError:
            dataset = h5py.File(self.path, 'w')

        if len(dataset):
            logging.debug(
                'Multiscan file already has {}.'.format(', '.join(dataset))
            )
        else:
            logging.debug('Starting with empty multiscan file.')

        for scancode in self.scancodes:
            scan = ScanSignature(
                scandatetime=self.multiscandatetime, scancode=scancode,
            ).get_scan()
            if scan is None or scancode in dataset:
                continue
            self._add(dataset=dataset, scan=scan)

        dataset.close()
        return h5py.File(self.path, 'r')


class Composite(object):

    def __init__(self, compositedatetime, scancodes, declutter):

        self.scancodes = scancodes
        self.declutter = declutter

        if None in declutter.values():
            raise ValueError('Declutter may not contain None values.')

        self.multiscan = MultiScan(
            multiscandatetime=compositedatetime,
            scancodes=scancodes,
        )

    def _calculate(self, datasets):
        """
        Return a composite dataset based on the
        weighted lowest altitudes method.
        """
        if not datasets:
            return np.ma.array(
                np.zeros(BASEGRID.get_shape()),
                mask=True,
                fill_value=config.NODATAVALUE,
            )

        # Read datasets
        metadata = [dict(ds.attrs) for ds in datasets]

        stations = [m['station'] for m in metadata]

        anth = np.array(
            [json.loads(m['antenna_height']) for m in metadata],
        ).reshape((-1, 1, 1)) / 1000

        rain = np.ma.array(
            [gridtools.h5ds2ma(ds['rain']) for ds in datasets],
            fill_value=config.NODATAVALUE,
        )

        rang = np.ma.array(
            [gridtools.h5ds2ma(ds['range']) for ds in datasets],
            fill_value=config.NODATAVALUE,
        )

        elev = np.ma.array(
            [gridtools.h5ds2ma(ds['elevation']) for ds in datasets],
            fill_value=config.NODATAVALUE,
        )

        # Extend mask around NL stations
        if 'NL60' in stations and 'NL61' in stations:
            for i in map(stations.index, ['NL60', 'NL61']):
                rain.mask[i][np.less(rang[i], 15)] = True

        # Do history declutter
        if self.declutter['history']:  # None or 0 disables history declutter
            if 'NL60' in stations and 'NL61' in stations:
                logging.debug('Starting history declutter.')
                h5 = h5py.File(
                    os.path.join(config.SHAPE_DIR, 'clutter.h5'), 'r',
                )

                logging.debug('Using clutter threshold of {}'.format(
                    self.declutter['history'],
                ))
                logging.debug('Clutterstats:')
                logging.debug(
                    '   count: {}'.format(h5.attrs['cluttercount']),
                )
                logging.debug('   range: {}'.format(h5.attrs['range']))

                # Initialize clutter array of same dimensions as rain array
                clutter = np.zeros(rain.shape, rain.dtype)
                for i, radar in enumerate(stations):
                    if radar in h5:
                        clutter[i] = h5[radar]

                # Determine the mask:
                cluttermask = np.logical_and(
                    np.equal(clutter, clutter.max(0)),
                    np.greater(clutter, self.declutter['history']),
                )

                for i, radar in enumerate(stations):
                    logging.debug('Masking {} pixels for {}'.format(
                        cluttermask[i].sum(), radar,
                    ))

                # Extend rain mask with cluttermask.
                rain.mask[cluttermask] = True
                h5.close()
            else:
                logging.debug('Need both NL60 and NL61 for history declutter.')

        rang.mask = rain.mask
        elev.mask = rain.mask

        # Calculate heights
        theta = calc.calculate_theta(
            rang=rang, elev=np.radians(elev), anth=anth,
        )

        alt_min = calc.calculate_height(
            theta=theta, elev=np.radians(elev - 0.5), anth=anth,
        )
        alt_max = calc.calculate_height(
            theta=theta, elev=np.radians(elev + 0.5), anth=anth,
        )

        lowest_alt_max = np.ma.where(
            np.equal(alt_min, alt_min.min(0)),
            alt_max,
            np.ma.masked,
        ).min(0)

        bandwidth = alt_max - alt_min

        vertdist1 = (lowest_alt_max - alt_min)
        vertdist1[np.ma.less(vertdist1, 0)] = 0

        vertdist2 = (lowest_alt_max - alt_max)
        vertdist2[np.ma.less(vertdist2, 0)] = 0

        overlap = vertdist1 - vertdist2

        weight = overlap / bandwidth

        composite = (rain * weight).sum(0) / weight.sum(0)
        composite.fill_value = config.NODATAVALUE

        return composite

    def _metadata(self, radars, datasets):
        """ Return metadata dict. """
        requested_stations = json.dumps(self.scancodes)
        timestamp = self.multiscan.multiscandatetime.strftime(
            config.TIMESTAMP_FORMAT,
        )
        metadatas = [dict(ds.attrs) for ds in datasets]
        stations = json.dumps(radars)
        locations = json.dumps([json.loads(metadata['location'])
                                for metadata in metadatas])
        ranges = json.dumps([json.loads(metadata['max_range'])
                             for metadata in metadatas])
        method = 'weighted lowest altitudes'
        declutter_size = str(self.declutter['size'])
        declutter_history = str(self.declutter['history'])

        return dict(
            requested_stations=requested_stations,
            stations=stations,
            locations=locations,
            ranges=ranges,
            method=method,
            timestamp=timestamp,
            declutter_size=declutter_size,
            declutter_history=declutter_history,
        )

    def _dataset(self, ma, md):
        """ Return dataset with composite. """
        dataset = BASEGRID.create_dataset()
        dataset.SetMetadata(md)
        band = dataset.GetRasterBand(1)
        band.WriteArray(ma.filled())
        band.SetNoDataValue(ma.fill_value)
        return dataset

    def get(self):
        """
        Return a composite dataset based on the
        weighted lowest altitudes method.
        """
        multiscan_dataset = self.multiscan.get()
        if len(multiscan_dataset):
            (
                radars,
                datasets,
            ) = zip(*[(code, dataset)
                    for code, dataset in multiscan_dataset.items()
                    if code in self.scancodes])
        else:
            radars, datasets = [], []

        logging.debug('Calculating composite.')

        if len(datasets) == 0:
            logging.warn(
                'no composite created for {}: None of {} found.'.format(
                    self.multiscan.multiscandatetime,
                    ', '.join(self.multiscan.scancodes),
                ),
            )
            pass
            # return None

        ma = self._calculate(datasets)
        md = self._metadata(radars=radars, datasets=datasets)

        multiscan_dataset.close()

        if self.declutter['size']:  # None or 0 disables history declutter
            calc.declutter_by_area(array=ma, area=self.declutter['size'])

        dataset = self._dataset(ma=ma, md=md)

        logging.info('Created composite {}: {}'.format(
            self.multiscan.multiscandatetime.strftime('%Y-%m-%d %H:%M:%S'),
            ', '.join(json.loads(dataset.GetMetadataItem(b'stations'))),
        ))

        return dataset


def make_aggregate(aggregate):
    aggregate.make()
    return aggregate


class Aggregate(object):
    """
    The Aggregate contains the sum of a number of radar composites,
    depending on the timeframe.
    
    It is the aggregate that is calibrated using ground gauges. The
    aggregate uses the composites of 5 minutes earlier, because the
    composite datetime is when the radar starts to scan, and the detected
    rain still has to fall down.
    """
    CODE = {
        datetime.timedelta(minutes=5): '5min',
        datetime.timedelta(hours=1): 'uur',
        datetime.timedelta(days=1): '24uur',
        datetime.timedelta(days=2): '48uur',
    }

    TD = {v: k for k, v in CODE.items()}

    SUB_CODE = {
        '48uur': '24uur',
        '24uur': 'uur',
        'uur': '5min',
    }

    def __init__(self, dt_aggregate, td_aggregate, scancodes, declutter):

        self.dt_aggregate = dt_aggregate
        self.td_aggregate = td_aggregate
        self.scancodes = scancodes
        self.code = self.CODE[td_aggregate]

        # Coerce self.declutter to requirement for Composite object
        if declutter is None or declutter['size'] is None:
            declutter_size = config.DECLUTTER_SIZE
        else:
            declutter_size = declutter['size']

        if declutter is None or declutter['history'] is None:
            declutter_history = config.DECLUTTER_HISTORY
        else:
            declutter_history = declutter['history']

        self.declutter = dict(size=declutter_size, history=declutter_history)
        # Prevent illegal combinations of dt and dd, can be nicer...
        if self.code == '48uur':
            if dt_aggregate != dt_aggregate.replace(hour=8, minute=0,
                                                    second=0, microsecond=0):
                raise ValueError
        if self.code == '24uur':
            if dt_aggregate != dt_aggregate.replace(hour=8, minute=0,
                                                    second=0, microsecond=0):
                raise ValueError
        if self.code == 'uur':
            if dt_aggregate != dt_aggregate.replace(minute=0,
                                                    second=0, microsecond=0):
                raise ValueError
        if self.code == '5min':
            if dt_aggregate != dt_aggregate.replace(
                minute=(dt_aggregate.minute // 5) * 5,
                second=0,
                microsecond=0,
            ):
                raise ValueError

    def _sub_datetimes(self):
        """
        Return datetime generator of datetimes of the next aggregate
        resulution.

        Note that it ends at this aggregate's datetime.
        """
        step = self.TD[self.SUB_CODE[self.code]]
        end = self.dt_aggregate
        start = self.dt_aggregate - self.td_aggregate + step

        current = start
        while current <= end:
            yield current
            current += step

    def get_path(self):
        """ Return the file path where this aggregate should be stored. """
        path_helper = utils.PathHelper(basedir=config.AGGREGATE_DIR,
                                       code=self.code,
                                       template='{code}_{timestamp}.h5')
        return path_helper.path(self.dt_aggregate)

    def _check(self, h5):
        """ Return if h5 is consistent with requested parameters. """
        if set(h5.attrs['radars']) != set(self.scancodes):
            # Aggregate was made for a different set of radars
            raise ValueError('Unmatched radarset in existing aggregate.')
        if h5.attrs['declutter_history'] != self.declutter['history']:
            raise ValueError('Other history declutter setting in aggregate')
        if h5.attrs['declutter_size'] != self.declutter['size']:
            raise ValueError('Other size declutter setting in aggregate')
        if not h5.attrs['available'].all():
            if self.code != '5min':
                raise ValueError('Missing radars in existing aggregate')
            # Check if it is useful to create a new composite
            dt_composite = self.dt_aggregate - self.TD[self.code]
            files_available = []
            for radar in sorted(self.scancodes):
                files_available.append(os.path.exists(ScanSignature(
                    scancode=radar, scandatetime=dt_composite
                ).get_scanpath()))
            if not (h5.attrs['available'] == files_available).all():
                raise ValueError('Missing radars, but scanfiles exist.')
            else:
                logging.debug('Missing radars caused by missing scanfiles.')
        return

    def _create(self):
        """
        Create a new dataset from the composite.
        """
        # Create the composite
        dt_composite = self.dt_aggregate - self.TD[self.code]

        composite = Composite(compositedatetime=dt_composite,
                              scancodes=self.scancodes,
                              declutter=self.declutter).get()

        # Composite unit is mm/hr and covers 5 minutes. It must be in mm.
        fill_value = config.NODATAVALUE
        array = composite.GetRasterBand(1).ReadAsArray()
        mask = np.equal(array, fill_value)
        masked_array = np.ma.array(
            array, mask=mask, fill_value=fill_value,
        ) / 12

        composite_meta = composite.GetMetadata()

        # Create the data for the h5
        radars = sorted(
            [str(radar)
             for radar in json.loads(composite_meta['requested_stations'])],
        )
        radar_list = zip(json.loads(composite_meta['stations']),
                         json.loads(composite_meta['ranges']),
                         json.loads(composite_meta['locations']))
        locations_dict = {rad: loc for rad, rng, loc in radar_list}
        ranges_dict = {rad: rng for rad, rng, loc in radar_list}

        available = np.array([radar in ranges_dict for radar in radars])
        ranges = [ranges_dict.get(radar, fill_value) for radar in radars]
        locations = np.array([locations_dict.get(radar, 2 * [fill_value])
                              for radar in radars])

        h5_meta = dict(
            grid_projection=BASEGRID.projection,
            grid_extent=BASEGRID.extent,
            grid_size=BASEGRID.size,
            radars=radars,
            ranges=ranges,
            locations=locations,
            available=available,
            method=composite_meta['method'],
            declutter_history=float(composite_meta['declutter_history']),
            declutter_size=float(composite_meta['declutter_size']),
            timestamp_first_composite=composite_meta['timestamp'],
            timestamp_last_composite=composite_meta['timestamp'],
            composite_count=1,
            fill_value=fill_value,
        )

        h5_data = dict(
            precipitation=masked_array,
        )

        path = self.get_path()
        utils.save_dataset(h5_data, h5_meta, path)
        logging.info('Created aggregate {} ({})'.format(
            self.dt_aggregate, self.code
        ))

    def _merge(self, aggregates):
        """
        Return h5_dataset by merging iterable of aggregate objects.
        """
        if self.code == 'uur':
            # Make the iterables in parallel
            pool = multiprocessing.Pool()
            iterable = iter(pool.map(make_aggregate, aggregates))
            pool.close()
        else:
            # Make the iterables the usual way
            iterable = iter(map(make_aggregate, aggregates))

        aggregate = iterable.next()
        h5 = aggregate.get()

        meta = dict(h5.attrs)
        available = [meta['available']]

        array = h5['precipitation']
        fill_value = config.NODATAVALUE
        mask = np.equal(array, fill_value)
        masked_array = np.ma.array(array, mask=mask, fill_value=fill_value)

        h5.close()

        for aggregate in iterable:

            h5 = aggregate.get()

            array = h5['precipitation']
            fill_value = config.NODATAVALUE
            mask = np.equal(array, fill_value)
            masked_array = np.ma.array([
                masked_array,
                np.ma.array(array, mask=mask)
            ]).sum(0)

            for i in range(len(meta['radars'])):
                if meta['ranges'][i] == config.NODATAVALUE:
                    meta['ranges'][i] = h5.attrs['ranges'][i]
                if (meta['locations'][i] == 2 * [config.NODATAVALUE]).all():
                    meta['locations'][i] = h5.attrs['locations'][i]

            available.append(meta['available'])

            meta['composite_count'] += h5.attrs.get(
                'composite_count',
            )
            meta['timestamp_last_composite'] = h5.attrs.get(
                'timestamp_last_composite',
            )

            h5.close()

        meta['available'] = np.vstack(available)
        data = dict(precipitation=masked_array)
        path = self.get_path()

        utils.save_dataset(data, meta, path)
        logging.info('Created aggregate {} ({})'.format(
            self.dt_aggregate, self.code
        ))

    def make(self):

        """ Creates the hdf5 file corresponding to this objects. """
        logging.debug('Creating aggregate {} ({})'.format(
            self.dt_aggregate, self.code,
        ))
        path = self.get_path()

        # If there is already a good one, return it
        if os.path.exists(path):
            logging.debug('Checking if existing aggregate can be reused.')
            try:
                with h5py.File(path, 'r') as h5:
                    self._check(h5)
                logging.debug('Check ok.')
                return
            except KeyError as error:
                logging.debug('Check failed: {}.'.format(error))
            except ValueError as error:
                logging.debug('Check failed: {}.'.format(error))

        # So, now we really need to create an aggregate.
        sub_code = self.SUB_CODE.get(self.code)

        if sub_code is None:
            return self._create()

        # If there is a sub_code, return corresponding aggregates merged.
        td_aggregate = self.TD[sub_code]
        dt_aggregate = self._sub_datetimes()
        sub_aggregates = (Aggregate(dt_aggregate=dt_aggregate,
                                    td_aggregate=td_aggregate,
                                    scancodes=self.scancodes,
                                    declutter=self.declutter)
                          for dt_aggregate in self._sub_datetimes())

        return self._merge(aggregates=sub_aggregates)

    def get(self):
        """ Return opened h5 dataset in read mode. """
        path = self.get_path()
        try:
            return h5py.File(path, 'r')
        except IOError:
            logging.warn(
                'Creating aggregate {} ({}) because it did not exist'.format(
                    self.dt_aggregate, self.code,
                ),
            )
            self.make()
        return h5py.File(path, 'r')
