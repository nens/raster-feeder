openradar
==========================================

Introduction

Usage, etc.

Scripts
-------
Scripts can be found in openradar/scripts

Scripts have an option --direct, to run without the message system.
Tasks have an argument cascade. For most scripts this means creating tasks for logical next steps. For the publish task, it means 'publish any rescaled products as well.' - it is usually safe to do if you have also run the calibrate task with 'cascade=True'. 
