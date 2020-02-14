Changelog of raster-feeder
==========================


0.7 (unreleased)
----------------

- Bump raster-store to 4.0.5, removes moving from flushing.

- Added more meaningful error when nowcast files missing.

- New Harmonie version.

- Add and use the REDIS_PASSWORD setting.

- Use python3, virtualenv, pytest.


0.6 (2019-07-24)
----------------

- Added ``touch-lizard`` script to update the raster in Lizard.

- Added HARMONIE scripts.

- Updated raster-store to fix the flushes.

- Enable average aggregations in nrr.

- Add subpackage for 'steps' forecasts.

- Refactor forecast store initialization and rotation to common module.

- Refactor to make nowcast a separate forecast product.

- Bumped numpy to 1.12.0.

- Fix logging for touch lizard, and add a short uuid to it.

- Make the meta setting template url configurable.

- Call touch_lizard from within rotate scripts.

- Implemented member selection based on statistics in steps.

- Add 'prcp' flavor of harmonie calculating intensity from cumulatives.

- Do not write 'group' json in create_tumbler, but print the geoblocks config
  instead.


0.5 (2016-12-23)
----------------

- Renamed atomic to nrr.

- Forked and stripped all openradar code.

- Added time_limit for all tasks.

- Worked towards new HDF5 format.

- Pinned numpy.


0.4 (2016-08-26)
----------------

- Add Jabbeke.

- Add basic nowcast product, task and script

- Improve image script product selection

- Remove groundfile code, add grounddata from database code

- Improved the atomic report code

- Many improvements to atomic scripts for raster store management

- Remove catch-all style exception handling from tasks

- Celery broker url configurable in settings

- Fix atomic move stop date when store end comes before chunk end

- Update of raster store to fix problem with simultaneous merge and store


0.3.7 (2013-08-29)
------------------

- Fixed independent volume fetching when one of the providers fails.


0.3.6 (2013-08-29)
------------------

- Speedup sync_radar_to_ftp by caching remote listings.


0.3.5 (2013-08-29)
------------------

- Add sync_radar_to_ftp script to fill holes from ftp outages.


0.3.4 (2013-06-17)
------------------

- Add ftp to default arguments for publishing.


0.3.3 (2013-06-11)
------------------

- Change max_age for fetching files to one day.


0.3.2 (2013-06-06)
------------------

- Fix products being published for product 'a' timeframe 'h'


0.3.1 (2013-02-14)
------------------

- Adjust unit and coerce value to float for opendap data.


0.3 (2013-02-14)
----------------

- Change threddsfile time:unit attribute to time:units

- Add opendap retrieval functionality to ThreddsFile

- Add function to retrieve data from multiple threddsfiles.


0.2.10 (2013-02-13)
-------------------

- Fix the last SHAPE_DIR references.


0.2.9 (2013-02-13)
------------------

- Correct path to osm image and remove from png making.


0.2.8 (2013-02-13)
------------------

- Fix bug regarding missing shapedir.


0.2.7 (2013-02-13)
------------------

- Major rewrite of configuration system. Most configuration is now moved to this package. Stuff may be broken now, but it enables standalone testing of this library in the future.


0.2.6 (2013-02-12)
------------------

- Publish a merged threddsfile as well, where the realtime products
  are overwritten by near-realtime products, etc. The available variable
  functions as a flag to indicate what data was written.

- Range is now an optional positional argument.


0.2.5 (2013-01-25)
------------------

- Create the animated gif as tempfile, then move to actual target.


0.2.4 (2013-01-24)
------------------

- Add animated gif creation.


0.2.3 (2013-01-22)
------------------

- Add script for syncing of ftp folders (hirlam, eps)


0.2.2 (2013-01-21)
------------------

- Even less crashing of threddsfiles.


0.2.1 (2013-01-18)
------------------

- No logging for FtpImporter if nothing fetched.

- Fix crashing when creating new threddsfiles.


0.2.0 (2013-01-17)
------------------

- Format logging and show ftp result summary in logfile.

- No longer prepare google gtiff for web viewer.

- Delete existing h5 when creating, even if opening in 'w' mode.


0.1.9 (2013-01-17)
------------------

- Add ftp info to config

- Add FtpImporter that imports directly from scanfile sources.


0.1.8 (2013-01-17)
------------------

- CSV is now read from zipfile if it exists.

- Organize now moves anything with a csv extension to a zipped csv.


0.1.7 (2013-01-16)
------------------

- Separate error handling for creation and publishing.

- Update existing threddsfiles when publishing.

- Moving to much larger threddsfiles of around 10000 grids.


0.1.6 (2013-01-10)
------------------

- Re-enable multiprocessing.

- Fix n, h products not being published.

- Do local copying to subfolders per product.


0.1.5 (2013-01-10)
------------------

- Improved publishing routine. Will publish in order of importance.

- Created FtpPublisher that takes care of using folders on ftp.

- Tweaks to log messages.


0.1.4 (2013-01-09)
------------------

- Fix bug that tried to make d product at 9


0.1.3 (2013-01-09)
------------------

- Fix bug in get method of ConsistentProduct.


0.1.2 (2013-01-09)
------------------

- Set threshold for rain to 0.008 (that is 0.1 / 12)


0.1.1 (2013-01-09)
------------------

- Add master script.


0.1 (2013-01-09)
----------------

- Initial project structure created with nensskel 1.30.dev0.

- Add code from nens/radar project.
