openradar
==========================================

Radar
-----
This repository was made for a project to improve the rain monitoring for 
national precipitation in the Netherlands. The rain forecast and monitoring
was believed to thoroughly be improved with the use of more than just the
two Dutch radar stations:
    * De Bilt (Lat: 52.10168, Lon: 5.17834) and 
    * Den Helder (Lat: 52.95334, 4.79997) 
by using relevant radar stations of the neighbouring countries Belgium and Germany.

The project lives at http://nationaleregenradar.nl

The master script ``bin/master`` organizes and aggregates data (if necessary), 
both radar and gauge (ground stations) data. The products are delivered 
real-time, near-real-time and afterwards. Every product is delivered for
different time-steps: 5 minutes, 1 hour and 24 hours. 

Every 5 minutes data is collected from the different radars and gauge stations. 
Especially rain gauge data is not always delivered for that time interval. The
near-real-time data product thus should give more reliable data as more data
has arrived at that time. The aggregates (hourly and daily) are also used to 
calibrate the 5 minute data.

Installation
------------
Install using system repositories (e.g. apt-get, brew, pacman):
    * python-gdal
    * python-matplotlib
    * python-pandas
    * python-psycopg2
    * python-rpy2
    * python-scipy
    * python-tornado

    * libgeos-dev
    * libhdf5-serial-dev
    * libnetcdf-dev

    * imagemagick

Then, to install the 'gstat' package, in the R interpreter::
    
    > install.packages('gstat')

The standard buildout deployment::
    
    $ python bootstrap.py
    $ bin/buildout

Then to setup the radar server production use, for Nelen & Schuurmans
the easiest way is to clone the nens/radar repository as development
package and symlink the necessary configuration files::
    
    $ bin/develop checkout radar
    $ bin/buildout  # Again, yes.
    $ ln -s src/radar/misc var/misc
    $ ln -s src/radar/radar/productionconfig.py openradar/localconfig.py

Scripts
-------
Scripts can be found in openradar/scripts

Scripts have an option --direct, to run without the task system.
Tasks have an argument --cascade. For most scripts this means creating
tasks for logical next steps. For the publish task, it means 'publish
any rescaled products as well.'

TODO: cover sync* scripts and partial scripts here, too.

Timezone
--------
Timezones:
- The time zones for all of the data is in UTC time.

Clutter filter
--------------
To update the clutter filter, execute this command::
    
    bin/clutter YYYYMMDD-YYYYMMDD -t ./my-clutter-file.h5

Put this file in the misc directory and update DECLUTTER_FILEPATH to
point to this file. The basename is enough, but an absolute path will
probably work, too.

Troubleshooting
---------------
If there is a hickup in the production and the task queue gets congested,
try purging it::

    $ bin/celery --app=openradar.tasks.app purge

Now, the realtime products are a good indication for the times at which
master execution has not succesfully completed. To get a list of missing
products in the past 7 days run::

    $ bin/repair 7d

To get a hint about which masters to re-run.
