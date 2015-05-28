How to convert the process for streamlining radar storage
---------------------------------------------------------
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
  - copy cronjobs from staging


2. Stop activity.
3. Rename stores
4. Init new stores
3. Leave current q, add it to the group
4. Make another final-like store, try copy all at once from q to semifinal
5. Copy final to semifinal and swap with finals
6. Repeat for day, hour, 5min

Later:

Tasks to be replaced by seach-and-fix scripts:
- Aggregate when needed
- Calibrate when needed
- Rescale when needed

Special thing is publish:
- Need a special published state somewhere. If not, publish on all old-fashioned channels again such as ftp, thredds, images.

Store and others:
- move atomic report to radar-task server
- remove history checkout
- document this new radar procedure in master readme


Roadmap to atomic infrastructure
--------------------------------
Rewrite to atomic:
    - aggregation
    - calibration
    - publication (how make sure target copy happens only once?)

Improve:
    - precalculate interpolations for all scans of NL and BE radars
    - double resolution 
    - clutter removal by scan comparison
    - get rid of matplotlib; we now use image.draw and nens-colormaps
