How to convert the process for streamlining radar storage
---------------------------------------------------------
Production current repo states:
    /srv/history:   4d5f25dc099dc85abd1c8e0ec04cdae43c7baed5
    /srv/openradar: f19ca6eabc280cd1cb632d09ae549feb5d89ae25

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
  - rename q to final_past_old
  - copy final to final_past_new
  - copy group configs from staging groups to production groups
  - add final_past_old and final_past_new to these configs
  - check with store-info
  - mkey all
- verify all works fine
- copy cronjobs from staging
- verify all works fine
- For 5min, hour and day:
  - store-put final_past_old final_past_new
  - remove final_past_old from group config
  - check with store-info
  - stop all cronjobs
  - mv final final_future
  - mv final_past_new final
  - store-put final_future final
  - rm -r final_future
  - remove final_past_new from group config
  - check with store-info
  - check all is very well
  - remove final_past_old
- move atomic report to radar-task server
- document this new radar procedure in master readme

Tasks to be replaced by search-and-fix scripts:
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
