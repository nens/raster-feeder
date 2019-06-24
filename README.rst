raster-feeder
==========================================

This software defines a number of commandline scripts to retrieve and load
raster data from a variety of sources into raster stores. Although largely
separated into separate components that have its own directories, they share a
a number of properties such as the use of the turn library for queueing and
putting the data on the same shared storage. 

Development installation
------------------------

$ docker-compose build
$ docker-compose up

(docker)$ buildout

Server installation
-------------------

Global dependencies::

    $ sudo apt install libnetcdf-dev libhdf5-serial-dev python-grib python-pip
    $ sudo pip install --upgrade setuptools zc.buildout


Trick buildout sysegg into thinking pygdal is available::

    $ sudo ln -s \
           /usr/lib/python2.7/dist-packages/GDAL-1.10.1.egg-info \
           /usr/lib/python2.7/dist-packages/pygdal-1.10.1.egg-info

Setting up::
    
    $ buildout


Configuration files
-------------------

A note of warning regarding configuration files. The separate components each
define their own config files, but can import certain variables from a central
config file in the parent directory. Both the central config and the component
config files try to import from a respective localconfig which makes the
configuration a bit complex. Be aware.


NRR
---

Scripts to feed local NRR precipitation datafiles into a group of raster stores
that enable efficient access of data over the complete growing dataset. Since
the NRR data comes in different quality types of varying batch sizes, at first
each quality type is stored in its own raster-store. A separate command merges
them into an intermediate raster-store in a quality-aware way. Finally, a move
command moves them to yet another raster-store for final storage.

This process takes place for each of the NRR time resolutions, (f)ive minutes,
(h)our and (d)ay. For initialization, run::

    $ bin/nrr-init

The following cronjobs should be installed on the production server to
make everything work::

    # Load radar data into the raster store
    # m    h dom mon dow command
    4-59/5 * *   *   *   /srv/raster-feeder/bin/nrr-store 1h -d -p r
    15     * *   *   *   /srv/raster-feeder/bin/nrr-store 1d -d -p n
    16     * *   *   *   /srv/raster-feeder/bin/nrr-store 7d -d -p a
    17     * *   *   *   /srv/raster-feeder/bin/nrr-store 7d -d -p u

    # Optimize radar data in the raster store
    # m    h dom mon dow command
    08     * *   *   *   /srv/raster-feeder/bin/nrr-move 5min real1 real2
    01    22 *   *   *   /srv/raster-feeder/bin/nrr-merge
    11    23 *   *   *   /srv/raster-feeder/bin/nrr-move 5min merge final
    21    23 *   *   1   /srv/raster-feeder/bin/nrr-move hour merge final
    31    23 1   *   *   /srv/raster-feeder/bin/nrr-move day merge final


A report script is included to check the filling state of the stores and to
report in case of missing products::
    
    # Report on the status of the data in the raster stores
    # m    h dom mon dow command
    0     12 *   *   *   /srv/raster-feeder/bin/nrr-report 7d -q
    */15   * *   *   *   /srv/raster-feeder/bin/nrr-report 7d


Forecasts
---------

Forecasts are made available in groups of two raster-stores. Per product two
commands are available, one for initialization of the store group, and another
one for the rotation of the stores using externally obtained data. Location of
the target group and the connection details of the suppliers are to be entered
in the respective localconfig.py of the subpackage.

ALARMTESTER: Worldwide from -3 to 10 and back with 5-minute resolution.
NOWCAST: 3 hour NRR extrapolation with 5-minute resolution.
HARMONIE: 48 hour model forecast with 1-hour resolution.
STEPS: 1 hour model forecast with 1-hour resolution.

To create the group of rotating stores (per product)::

    $ bin/alarmtester-init
    $ bin/nowcast-init
    $ bin/harmonie-init
    $ bin/steps-init

To have the stores automatically rotate at predetermined times, use crontab::

    # Rotate forecast stores
    # m    h      dom mon dow command
    0      *      *   *   *   /srv/raster-feeder/bin/alarmtester-rotate
    */5    *      *   *   *   /srv/raster-feeder/bin/nowcast-rotate
    19     5-23/6 *   *   *   /srv/raster-feeder/bin/harmonie-rotate
    25-29,55-59 * *   *   *   /srv/raster-feeder/bin/steps-rotate  # aligned with model runs

On staging, we use "\*/5" for the alarmtester to be able to test every 5 minutes.


Informing Lizard of changes to stores
-------------------------------------
Lizard RasterStore-objects will not be aware of changes by scripts defined
here. Therefore a script is available to do exactly that, which may be
incorporated in relevant cronjob lines::

    $ bin/touch-lizard <uuid>

Forecast subpackages also offer a TOUCH_LIZARD setting that can be overridden
in the localconfig to specify uuids to touch right after rotation.


TODO
----
- Generic FTP downloader in common module, possibly after the steps server
  class.

LOCAL SOURCES AND STORES
------------------------

TODO Description

- Files must be placed in `/var/local/source/`.
- A flag can move to procssed the process files to avoid conflicting just if task went well.
