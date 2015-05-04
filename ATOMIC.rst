How to convert the process for streamlining radar storage
---------------------------------------------------------

Stop production storing.
Promote everything into the stores that stay.
Manually rename q stores to final, for three groups
Checkout new system
Run init
Start storing
Start moving, make conjobs for it


Roadmap to atomic infrastructure
--------------------------------

handle:
    - multiprocessing still available?
    - why not even amounts of sources into radar store?
    - handle deadlocks in throttle

simplify:
    - get rid of celery, we now use throttle
    - get rid of matplotlib; we now use image.draw and nens-colormaps

rewrite to atomic:
    - ftp retrieve
    - aggregate
    - calibrate
    - publish

improve:
    - use stores for all aggregates and calibrates
    - precalculate interpolations for all scans of all radars
    - double resulution 
    - clutter removal by scan comparison

