raster-feeder
==========================================

This software defines a number of commandline scripts to retrieve and load
raster data from a variety of sources into raster stores. Although largely
separated into separate components that have its own directories, they share a
a number of properties such as the use of the turn library for queueing and
putting the data on the same shared storage. 

installation
------------

Global dependencies::

    $ sudo apt install libhdf5-serial-dev python-grib

Setting up::
    
    $ python bootstrap.py
    $ bin/buildout


Configuration files
-------------------
A note of warning regarding configuration files. The separate components each
define their own config files, but can import certain variables from a central
config file in the parent directory. Both the central config and the component
config files try to import from a respective localconfig which makes the
configuration a bit complex. Be aware.


NRR
---

Scripts to feed NRR precipitation data into a group of raster stores that
enable efficient access of data over the complete growing dataset. For
initialization, run::

    $ bin/nrr-init

The following cronjobs should be installed on the production server to
make everything work::

    # m    h dom mon dow command
    # Load radar data into the raster store
    */5    * *   *   *   /srv/raster-feeder/bin/nrr-nowcast
    4-59/5 * *   *   *   /srv/raster-feeder/bin/nrr-store 1h -d -p r
    15     * *   *   *   /srv/raster-feeder/bin/nrr-store 1d -d -p n
    16     * *   *   *   /srv/raster-feeder/bin/nrr-store 7d -d -p a
    17     * *   *   *   /srv/raster-feeder/bin/nrr-store 7d -d -p u
    # Optimize radar data in the raster store
    08     * *   *   *   /srv/raster-feeder/bin/nrr-move 5min real1 real2
    01    22 *   *   *   /srv/raster-feeder/bin/nrr-merge
    11    23 *   *   *   /srv/raster-feeder/bin/nrr-move 5min merge final
    21    23 *   *   1   /srv/raster-feeder/bin/nrr-move hour merge final
    31    23 1   *   *   /srv/raster-feeder/bin/nrr-move day merge final
    # Report on the status of the data in the raster stores
    0     12 *   *   *   /srv/raster-feeder/bin/nrr-report 7d -q
    */15   * *   *   *   /srv/raster-feeder/bin/nrr-report 7d


HARMONIE
--------

Scripts to feed NRR precipitation data into a group of raster stores that
enable efficient access of data over the complete growing dataset. For
initialization, run::

    $ bin/harmonie-init

The following cronjobs should be installed on the production server to
make everything work::

    # m    h dom mon dow command
    # Rotate the HARMONIE stores
    19 5-23/6 *   *   *   /srv/raster-feeder/bin/harmonie-rotate


TODO
----

- Generic FTP downloader in common module.
- Generic rotating group init function in common module.
- Remove nowcast from 5min and place in separate init.
