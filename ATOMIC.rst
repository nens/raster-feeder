How to convert the process for streamlining radar storage
---------------------------------------------------------
From /srv/history(arjan-boxes):
- Remove store related cronjobs
- Verify updating has stopped
- For 5min, hour and day:
    - atomic-promote everything to the q stores
    - verify with store-info
    - rm -r z and u stores
    - mv q final
From /srv/openradar(master):
  - git pull, python bootstrap.py, bin/buildout
  - bin/atomic-init
  - copy cronjobs from staging


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
