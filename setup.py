from setuptools import setup

version = '0.7.dev0'

long_description = '\n\n'.join([
    open('README.rst').read(),
    open('CREDITS.rst').read(),
    open('CHANGES.rst').read(),
    ])

install_requires = [
    'ciso8601',
    'dask_geomodeling',
    'h5py>=2.3.1',
    'mock',
    'netCDF4',
    'numpy',
    'pygdal',
    'pygrib',
    'raster-store',
    'redis',
    'requests',
    'scipy',
    'sentry-sdk',
    'setuptools',
    'turn',
    ],

tests_require = ["ipdb", "ipython", "pytest", "pytest-cov"]

setup(name='raster_feeder',
      version=version,
      description=("Scripts to feed and optimize "
                   "realtime temporal data into raster stores."),
      long_description=long_description,
      # Get strings from http://www.python.org/pypi?%3Aaction=list_classifiers
      classifiers=[],
      keywords=[],
      author='Arjan Verkerk',
      author_email='arjan.verkerk@nelen-schuurmans.nl',
      url='',
      license='GPL',
      packages=['raster_feeder'],
      include_package_data=True,
      zip_safe=False,
      install_requires=install_requires,
      tests_require=tests_require,
      extras_require={"test": tests_require},
      entry_points={
          'console_scripts': [
              # COMMON
              'info = raster_feeder.info:main',
              # NRR
              'nrr-init = raster_feeder.nrr.init:main',
              'nrr-merge = raster_feeder.nrr.merge:main',
              'nrr-move = raster_feeder.nrr.move:main',
              'nrr-report = raster_feeder.nrr.report:main',
              'nrr-store = raster_feeder.nrr.store:main',
              # NOWCAST
              'nowcast-init = raster_feeder.nowcast.init:main',
              'nowcast-rotate = raster_feeder.nowcast.rotate:main',
              # HARMONIE
              'harmonie-init = raster_feeder.harmonie.init:main',
              'harmonie-rotate = raster_feeder.harmonie.rotate:main',
              # STEPS
              'steps-init = raster_feeder.steps.init:main',
              'steps-rotate = raster_feeder.steps.rotate:main',
              # ALARMTESTER
              'alarmtester-init = raster_feeder.alarmtester.init:main',
              'alarmtester-rotate = raster_feeder.alarmtester.rotate:main',
              # MISC
              'touch-lizard = raster_feeder.touch:main',
          ]},
      )
