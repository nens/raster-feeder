How to convert the process for streamlining radar storage
---------------------------------------------------------
Locally with staging raster mount:
- Test the working of meta with raster-store 1.5 and staging data
- Update raster-store to 1.5 in master openradar
- Verify everything works o.k. on staging task server

On production server:
From /srv/history(arjan-boxes):
- Remove store related cronjobs
- Verify updating has stopped
- For 5min, hour and day:
    - atomic-promote everything to the q stores
    - verify with store-info
    - rm -r z and u stores

From /srv/openradar(master):
- git pull, python bootstrap.py, bin/buildout
- bin/atomic-init
- For 5min, hour and day:
  - copy final to transfer
  - copy group config from staging groups to production groups
  - add final and q to these
- copy cronjobs from staging
- move atomic report to radar-task server
- document this new radar procedure in master readme


Tasks to be replaced by seach-and-fix scripts:
- Aggregate when needed
- Calibrate when needed
- Rescale when needed
- Special thing is publish:
  - Need a special published state somewhere. If not, publish on all
    old-fashioned channels again such as ftp, thredds, images.

Improve:
- precalculate interpolations for all scans of NL and BE radars
- double resolution 
- clutter removal by scan comparison
- get rid of matplotlib; we now use image.draw and nens-colormaps
