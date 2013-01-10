#!/usr/bin/python
# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from osgeo import gdal

import argparse
import calendar
import datetime
import ftplib
import h5py
import logging
import numpy as np
import os
import shutil
import tempfile

from radar import config

from openradar import log
from openradar import utils
from openradar import scans
from openradar.interpolation import DataLoader, Interpolator

log.setup_logging()


class ThreddsFile(object):
    """
    Contains a number of products in h5 format.
    """
    REPLACE_KWARGS = dict(
        f=dict(minute=0),
        h=dict(hour=0),
        d=dict(day=1),
    )
    def __init__(self, datetime, product, timeframe, consistent=False):
        """
        Raise ValueError if date does not match with product and timeframe
        """
        # Some inits.
        if consistent:
            self.SOURCE_DIR = config.CONSISTENT_DIR
        else:
            self.SOURCE_DIR = config.CALIBRATE_DIR

        self.timeframe = timeframe
        self.product = product
        self.datetime = datetime
        self.timedelta = config.TIMEFRAME_DELTA[self.timeframe]

        self.path = utils.PathHelper(
            basedir=config.THREDDS_DIR,
            code=config.PRODUCT_CODE[self.timeframe][self.product],
            template=config.PRODUCT_TEMPLATE,
        ).path(datetime)

        self.steps = dict(
            f=12,
            h=24,
            d=calendar.monthrange(
                self.datetime.year, self.datetime.month,
            )[0],
        )[self.timeframe]

        # Check if supplied date is valid at all.
        if datetime.microsecond == 0 and datetime.second == 0:
            if datetime.minute == (datetime.minute // 5) * 5:
                if timeframe == 'f':
                    return
                if datetime.minute == 0:
                    if timeframe == 'h':
                        return
                    if datetime.hour == 8:
                        if timeframe == 'd':
                            return

        raise ValueError('Datetime incompatible with timeframe.')

    @classmethod
    def get(cls, product):
        """
        Return instance of threddsfile where this product belongs in.
        """
        replace_kwargs = cls.REPLACE_KWARGS[product.timeframe]
        return cls(
            datetime=product.date.replace(**replace_kwargs),
            product=product.product,
            timeframe=product.timeframe,
            consistent=isinstance(product, ConsistentProduct)
        )

    def _sources(self):
        """
        Return a list of paths of products which should exist to create
        this.  Later, if interfaces have improved, this could as well
        become a list of product objects.
        """

        path_helper = utils.PathHelper(
            basedir=self.SOURCE_DIR,
            code=config.PRODUCT_CODE[self.timeframe][self.product],
            template=config.PRODUCT_TEMPLATE,
        )

        for i in range(self.steps):
            source_datetime = self.datetime + i * self.timedelta
            yield source_datetime, path_helper.path(source_datetime)

    def _data(self):
        """ Return precipitation, available, timestamp. """
        shape = scans.BASEGRID.get_shape() + (self.steps,)
        precipitation = np.ma.array(
            np.empty(shape), mask=True, dtype=np.float32,
            fill_value=config.NODATAVALUE,
        )
        available = []
        datetimes = []

        for i, (datetime, path) in enumerate(self._sources()):
            datetimes.append(datetime)
            try:
                with h5py.File(path, 'r') as h5:
                    precipitation[..., i] = h5['precipitation'][...]
                available.append(True)
            except IOError:
                available.append(False)
        logging.debug('Thredds fill status:' + ''.join([str(int(a)) for a in available]))
        return precipitation, available, datetimes

    def update(self):
        """ Create a temporary new h5 file and move it in place."""
        precipitation, available, datetimes = self._data()

        h5_temp_path = tempfile.mkstemp()[1]
        with h5py.File(h5_temp_path) as h5:

            # East
            east = scans.BASEGRID.get_grid()[0][0]
            dataset = h5.create_dataset(
                'east', east.shape, east.dtype,
                compression='gzip', shuffle=True,
            )
            dataset[...] = east
            
            # North
            north = scans.BASEGRID.get_grid()[1][:, 0]
            dataset = h5.create_dataset(
                'north', north.shape, north.dtype,
                compression='gzip', shuffle=True,
            )
            dataset[...] = north

            # Time
            reference_datetime = self.datetime.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            dataset = h5.create_dataset(
                'time', [len(datetimes)], np.uint32,
                compression='gzip', shuffle=True,
            )
            dataset.attrs['standard_name'] = b'time'
            dataset.attrs['long_name'] = b'time'
            dataset.attrs['calendar'] = b'gregorian'
            dataset.attrs['unit'] = reference_datetime.strftime(
                'seconds since %Y-%m-%d'
            )
            dataset[...] = [round((d - reference_datetime).total_seconds())
                            for d in datetimes]

            # Precipitation
            dataset = h5.create_dataset(
                'precipitation', precipitation.shape, precipitation.dtype,
                compression='gzip', shuffle=True,
            )
            dataset[...] = precipitation

            # Availability
            dataset = h5.create_dataset(
                'available', [len(available)], np.uint8,
                compression='gzip', shuffle=True,
            )
            dataset[...] = available
            
            # Dimensions
            h5['precipitation'].dims.create_scale(h5['north'])
            h5['precipitation'].dims.create_scale(h5['east'])
            h5['precipitation'].dims.create_scale(h5['time'])
            
            h5['precipitation'].dims[0].attach_scale(h5['north'])
            h5['precipitation'].dims[1].attach_scale(h5['east'])
            h5['precipitation'].dims[2].attach_scale(h5['time'])

            h5['available'].dims.create_scale(h5['time'])
            h5['available'].dims[0].attach_scale(h5['time'])
        


        utils.makedir(os.path.dirname(self.path))
        shutil.move(h5_temp_path, self.path)
        logging.info(
            'Updated ThreddsFile {}'.format(os.path.basename(self.path)),
        )
        logging.debug(self.path)

        """
        Updates threddsfile with available data.
        - Always create a complete file, which then replaces the old one.
        - datasets: preciptiation, available, timestamp.
        -

        - Append new data
        - If there are holes in the data
        """


class CalibratedProduct(object):
    '''
    Depending on the product requested the produce method will create:
    E.g. At 2012-12-18-09:05 the products that can be made at that moment
        depending on the product are:
            real-time           => 2012-12-18-09:05
            near-real-time      => 2012-12-18-08:05
            afterwards          => 2012-12-16-09:05
    '''

    def __init__(self, product, timeframe, date):
        self.prodcode = product
        self.product = self.prodcode  # Backwards compatible
        self.timeframe = timeframe
        self.groundfile_datetime = utils.get_groundfile_datetime(
            prodcode=product, date=date,
        )
        self.groundpath = scans.GroundData(
            datacode=config.GROUND_CODE[self.timeframe],
            datadatetime=self.groundfile_datetime,
        ).get_datapath()
        self.path = utils.PathHelper(
            basedir=config.CALIBRATE_DIR,
            code=config.PRODUCT_CODE[self.timeframe][self.product],
            template=config.PRODUCT_TEMPLATE,
        ).path(date)
        self.calibratepath = self.path  # Backwards compatible
        self.date = date

    def make(self):
        td_aggregate = config.TIMEFRAME_DELTA[self.timeframe]
        if self.groundfile_datetime.year < 2012:
            groundpath = os.path.join(config.SHAPE_DIR, 'tests/2011.csv')
        dataloader = DataLoader(
            metafile=os.path.join(config.SHAPE_DIR, 'grondstations.csv'),
            datafile=self.groundpath,
            date=self.date,
            delta=td_aggregate)
        try:
            dataloader.processdata()
            stations_count = len(dataloader.rainstations)
        except:
            logging.debug("Can't process data; there is none for: {}".format(
                self.groundfile_datetime))
            stations_count = 0
        interpolater = Interpolator(dataloader)
        try:
            mask = utils.get_countrymask()
            calibrated_radar = interpolater.get_calibrated()
            result = (mask * calibrated_radar + (1 - mask) *
                    interpolater.precipitation)
            self.calibrated =  result
        except:
            self.calibrated = interpolater.precipitation
            logging.warn("Taking the original radar data for date: {}".format(
                self.date))
        self.metadata = dict(dataloader.dataset.attrs)
        dataloader.dataset.close()
        self.metadata.update({'stations_count': stations_count})
        calibrated_ma = np.ma.array(
            self.calibrated,
            mask=np.equal(self.calibrated, config.NODATAVALUE),
        )
        calibrate = utils.save_dataset(
            dict(precipitation=calibrated_ma),
            self.metadata,
            self.calibratepath,
        )
        logging.info('Created CalibratedProduct {}'.format(
            os.path.basename(self.calibratepath)
        ))
        logging.debug(self.calibratepath)

    def get(self):
        try:
            return h5py.File(self.calibratepath, 'r')
        except IOError:
            logging.warn(
                    'Creating calibrated product {}, because it did not'
                    ' exist'.format(self.calibratepath))
            self.make()
        return h5py.File(self.calibratepath, 'r')

    def make_cfgrid(self):
        td_aggregate = config.TIMEFRAME_DELTA[self.timeframe]
        dataloader = DataLoader(
            metafile=os.path.join(config.SHAPE_DIR, 'grondstations.csv'),
            datafile=self.groundpath,
            date=self.datadatetime,
            delta=td_aggregate)
        dataloader.processdata()
        stations_count = len(dataloader.rainstations)
        interpolator = Interpolator(dataloader)
        interpolator.get_correction_factor()


class ConsistentProduct(object):
    """ Conisitified products are usually created by the consisitier. """

    def __init__(self, datetime, prodcode, timeframe):
        self.datetime = datetime
        self.date = datetime  # Backwards compatible
        self.prodcode = prodcode
        self.product = prodcode  # Backwards compatible
        self.timeframe = timeframe
        self.path = utils.PathHelper(
            basedir=config.CONSISTENT_DIR,
            code=config.PRODUCT_CODE[self.timeframe][self.prodcode],
            template=config.PRODUCT_TEMPLATE,
        ).path(datetime)
    
    def get(self):
        """ 
        Return h5 dataset opened in read mode.

        Crashes when the file does not exist. This should be catched by caller.
        """
        return h5py.File(self.path, 'r')

    @classmethod
    def create(cls, product, factor, consistent_with):
        """
        Return ConsistentProduct.
        
        Creates a ConsistentProduct from product with data multiplied
        by factor and adds consistent_with to the metadata.
        """
        # Create the consistent product object
        consistent_product = cls(
            datetime=product.date,
            prodcode=product.product,
            timeframe=product.timeframe,
        )

        # Create the h5 datafile for it
        with product.get() as h5:
            data = h5['precipitation']
            mask = np.equal(data, config.NODATAVALUE)
            data = dict(precipitation=np.ma.array(data, mask=mask) * factor)
            meta = dict(h5.attrs)
            meta.update(consistent_with=consistent_with)
        utils.save_dataset(
            data=data, 
            meta=meta, 
            path=consistent_product.path
        )

        # get() will now work, so return the object.
        filepath = consistent_product.path
        filename = os.path.basename(filepath)
        logging.info('Created consistent product {}'.format(filename))
        logging.debug('Created consistent product {}'.format(filepath))
        return consistent_product


class Consistifier(object):
    '''
    The products that are updated afterwards with new gaugestations need to
    be consistent with the aggregates of the same date.
    E.g. In the hour 12.00 there are 12 * 5 minute products and 1 one hour
    product. The 12 seperate 5 minutes need add up to the same amount of
    precipitation as the hourly aggregate.

    The consistent products are only necessary for 3 products:
        - 5 minute near-realtime
        - 5 minute afterwards
        - hour near-realtime

    To make the products one can initiate class and run make_consistent:
    cproduct = ConsistentProduct('n', 'f', 201212180700)
    cproduct.make_consistent()
    Products are written in config.CONSISTENT_DIR
    '''
    """ Is a class purily for encapsulation purposes."""
    SUB_TIMEFRAME = {'d': 'h', 'h': 'f'}
    
    
    @classmethod
    def _reliable(cls, product):
        """
        Return if product enables consistification of other products.
        """
        prodcode, timeframe = product.prodcode, product.timeframe
        if prodcode == 'a':
            if timeframe == 'd':
                return True
            if timeframe == 'h' and isinstance(product, ConsistentProduct):
                return True
        if prodcode == 'n' and timeframe == 'h':
            return True
        return False

    @classmethod
    def _subproduct_datetimes(cls, product):
        """ Return datetimes for subproducts of product. """
        amount_of_subproducts = dict(h=12, d=24)[product.timeframe]
        sub_timeframe = cls.SUB_TIMEFRAME[product.timeframe]
        sub_timedelta = config.TIMEFRAME_DELTA[sub_timeframe]
        for i in range(amount_of_subproducts):
            yield product.date + sub_timedelta * (i - amount_of_subproducts)

    @classmethod
    def _subproducts(cls, product):
        """ Return the CalibratedProducts to be consistified with product """
        sub_timeframe = cls.SUB_TIMEFRAME[product.timeframe]
        for datetime in cls._subproduct_datetimes(product):
            yield CalibratedProduct(date=datetime,
                                    product=product.product,
                                    timeframe=sub_timeframe)
        

    @classmethod
    def _precipitation_from_product(cls, product):
        """ Return precipitation as masked array. """
        with product.get() as h5:
            data = h5['precipitation']
            mask = np.equal(data, config.NODATAVALUE)
            precipitation = np.ma.array(data, mask=mask)
        return precipitation
        

    @classmethod
    def create_consistent_products(cls, product):
        """ Returns a list of consistent products that were created. """
        consistified_products = []
        if cls._reliable(product):
            # Calculate sum of subproducts
            subproduct_sum = np.ma.zeros(scans.BASEGRID.get_shape())
            for subproduct in cls._subproducts(product):
                    subproduct_sum = np.ma.sum([
                        subproduct_sum,
                        cls._precipitation_from_product(subproduct),
                    ], 0)
            
            # Calculate factor
            factor = cls._precipitation_from_product(product) / subproduct_sum

            # Create consistent products
            for subproduct in cls._subproducts(product):
                consistified_products.append(
                    ConsistentProduct.create(
                        product=subproduct,
                        factor=factor,
                        consistent_with = os.path.basename(subproduct.path)
                    )
                )
            # Run this method on those products as well, since some
            # consistified products allow for consistent products themselves,
            # For example a.d consistifies a.h which in turn consitifies a.f.
            more_consistified_products = []
            for consistified_product in consistified_products:
                more_consistified_products.extend(
                    cls.create_consistent_products(consistified_product)
                )
            consistified_products.extend(more_consistified_products)
        return consistified_products


class FtpPublisher(object):
    
    def __enter__(self):
        """ 
        Make the connection to the FTP server.
        
        If necessary, create the directories.
        """
        self.ftp = ftplib.FTP(config.FTP_HOST, config.FTP_USER, config.FTP_PASSWORD)
        # Create directories when necessary
        ftp_paths = self.ftp.nlst()
        for path in [path 
                     for d in config.PRODUCT_CODE.values()
                     for path in d.values()]:
            if not path in ftp_paths:
                self.ftp.mkd(path)
        
        return self
    
    def __exit__(self, exception_type, error, traceback):
        """ Close ftp connection. """
        self.ftp.quit()

    def publish(self, product):
        """ Publish the product in the correct folder. """
        ftp_file = os.path.join(
            config.PRODUCT_CODE[product.timeframe][product.prodcode],
            os.path.basename(product.path),
        )
        with open(product.path, 'rb') as product_file:
            response = self.ftp.storbinary(
                'STOR {}'.format(ftp_file),
                product_file,
            )

        logging.debug('ftp response: {}'.format(response))
        logging.debug(ftp_file)
        logging.info(
            'Stored FTP file {}'.format(os.path.basename(ftp_file)),
        )


def publish(products):
    """
    Each product is published to a number of locations.

    The difference between products and publications is that for some
    calibrated products, there also exists a consistent product. In that
    case, only the consistent product should be published.
    """
    # Get rid of the calibrated products that have a consistent equivalent
    publications = []
    for product in products:
        consistent_expected = utils.consistent_product_expected(
            product=product.product, timeframe=product.timeframe,
        )
        if consistent_expected and isinstance(product, CalibratedProduct):
            continue
        publications.append(product)

    logging.debug('{} publications out of {} products'.format(
        len(publications), len(products),
    ))
    logging.info('Start publishing {} publications.'.format(len(publications)))

    # Publish to target dirs as configured in config:
    logging.debug('Preparing {} dirs.'.format(len(config.COPY_TARGET_DIRS)))
    for target_dir in config.COPY_TARGET_DIRS:
        for path in [path 
                     for d in config.PRODUCT_CODE.values()
                     for path in d.values()]:
            utils.makedir(os.path.join(target_dir, path))
    logging.debug('Copying publications.')
    for product in publications:
        for target_dir in config.COPY_TARGET_DIRS:
            target_subdir = os.path.join(
                target_dir,
                config.PRODUCT_CODE[product.timeframe][product.prodcode],
            )
            shutil.copy(product.path, target_subdir)
    logging.info('Local target dir copying complete.')
        
    # Publish to FTP configured in config:
    if hasattr(config, 'FTP_HOST') and config.FTP_HOST != '':
        with FtpPublisher() as ftp_publisher:
            for product in publications:
                ftp_publisher.publish(product)
        logging.info('FTP publishing complete.')
    else:
        logging.warning('FTP not configured, FTP publishing not possible.')
    
    # Update thredds
    for product in publications:
        ThreddsFile.get(product=product).update()
    logging.info('Thredds publishing complete.')
   
    logging.info('Done publishing products.')
