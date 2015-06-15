How to convert the process for streamlining radar storage
---------------------------------------------------------
- Atomic move seems not totally aligned, see real1 => real2 movements
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
