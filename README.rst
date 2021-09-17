raster-feeder
=============

This software defines a number of commandline scripts to retrieve and load
raster data from a variety of sources into raster stores. Although largely
separated into separate components that have its own directories, they share a
a number of properties such as the use of the turn library for queueing and
putting the data on the same shared storage. 

Development installation
------------------------

For development, you can use a docker-compose setup::

    $ docker-compose build --build-arg uid=`id -u` --build-arg gid=`id -g` lib
    $ docker-compose up --no-start
    $ docker-compose start
    $ docker-compose exec lib bash

Create a virtualenv::

    # note that Dockerfile prepends .venv/bin to $PATH
    (docker)$ virtualenv .venv --system-site-packages

Install dependencies & package and run tests::

    (docker)$ pip install -r requirements.txt --index-url https://packages.lizard.net
    (docker)$ pip install -e .[test]
    (docker)$ pytest

Update packages::
    
    (docker)$ rm -rf .venv
    (docker)$ virtualenv --system-site-packages .venv
    (docker)$ pip install . --index-url https://packages.lizard.net
    (docker)$ pip freeze | grep -v threedidepth > requirements.txt


Server installation
-------------------

Global dependencies (apt)::

    git
    libhdf5-serial-dev
    python3-gdal
    python3-grib
    python3-pip

Installation::

    $ sudo pip3 install --upgrade pip virtualenv
    $ virtualenv --system-site-packages .venv
    $ source .venv/bin/activate
    (virtualenv)$ pip install -r requirements.txt --index-url https://packages.lizard.net --no-binary=h5py
    (virtualenv)$ pip install -e .


Configuration files
-------------------

A note of warning regarding configuration files. The separate components each
define their own config files, but can import certain variables from a central
config file in the parent directory. Both the central config and the component
config files try to import from a respective localconfig which makes the
configuration a bit complex. Be aware.

Private configurations and crontabs 
-----------------------------------

In the nens/rr-task a collection of configurations is available
to symlink to from this project, as well as crontabs for scheduling the various
scripts.

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

    $ .venv/bin/alarmtester-init
    $ .venv/bin/nowcast-init
    $ .venv/bin/harmonie-init
    $ .venv/bin/steps-init

And to rotate the data::

    $ .venv/bin/alarmtester-rotate
    $ .venv/bin/nowcast-rotate
    $ .venv/bin/harmonie-rotate
    $ .venv/bin/steps-rotate


Informing Lizard of changes to stores
-------------------------------------
Lizard RasterStore-objects will not be aware of changes by scripts defined
here. Therefore a script is available to do exactly that, which may be
incorporated in relevant cronjob lines::

    $ .venv/bin/touch-lizard <uuid>

Forecast subpackages also offer a TOUCH_LIZARD setting that can be overridden
in the localconfig to specify uuids to touch right after rotation.


TODO
----
- Generic FTP downloader in common module, possibly after the steps server
  class.


NRR (Deprecated)
----------------

NOTE nrr stores are no longer fed from raster-feeder, but directly pushed to
lizard as implemented via the code for nationalie-regenradar-v2.

Scripts to feed local NRR precipitation datafiles into a group of raster stores
that enable efficient access of data over the complete growing dataset. Since
the NRR data comes in different quality types of varying batch sizes, at first
each quality type is stored in its own raster-store. A separate command merges
them into an intermediate raster-store in a quality-aware way. Finally, a move
command moves them to yet another raster-store for final storage.

This process takes place for each of the NRR time resolutions, (f)ive minutes,
(h)our and (d)ay. Available commands::

    $ .venv/bin/nrr-init    # create stores and configs 
    $ .venv/bin/nrr-store   # store data from nrr files
    $ .venv/bin/nrr-move    # move data from one store in the group to another
    $ .venv/bin/nrr-merge   # merge data from sereveral stores to a single store
    $ .venv/bin/nrr-report  # report on the quality and / or completeness of
                            # stored data
