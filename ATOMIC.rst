How to convert the process for streamlining radar storage
---------------------------------------------------------
- For 5min, hour and day:
  - store-put final_past_old final_past_new
  - stop all cronjobs
  - replace final_past_old with final_past_new in radar group config
  - remove final_past_old from rasterserver raster group config
  - check with store-info
  - mv final final_future && mv final_past_new final
  - store-put final_future final
  - rm -rv final_future
  - remove final_past_new from group config
  - check with store-info
  - check all is very well
  - start all cronjobs
  - remove final_past_old completely (only after all three have been moved)
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
