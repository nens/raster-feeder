raster-feeder
==========================================

NRR
---
Scripts to feed NRR precipitation data into a group of raster stores
that enable efficient access of data over the complete growing dataset.

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
