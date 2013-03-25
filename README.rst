openradar
==========================================

Introduction

Usage, etc.

Installation
------------

The standard way::
    
    $ python bootstrap.py
    $ bin/buildout

Then to setup the radar server production use, for Nelen & Schuurmans
the easiest way is to clone the nens/radar repository as development
package and symlink the necessary configuration files::
    
    $ bin/develop checkout radar
    $ bin/buildout  # Again, yes.
    $ ln -s src/radar/misc
    $ ln -s src/radar/radar/productionconfig.py openradar/localconfig.py
    



Scripts
-------
Scripts can be found in openradar/scripts

Scripts have an option --direct, to run without the task system.
Tasks have an argument --cascade. For most scripts this means creating
tasks for logical next steps. For the publish task, it means 'publish
any rescaled products as well.'
