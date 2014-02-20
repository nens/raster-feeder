# -*- coding: utf-8 -*-
"""
Created on Tue Feb 18 14:44:37 2014

Tom van Steijn, RHDHV
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import os
import sys
import glob
import datetime as dt

import pandas as pd
import psycopg2

from openradar import config

def get_rainstations(datetime, timeframe):  
    """ Connect and query database for datetime and timeframe. """    
    
    # Define connection string and query template
    connstr_template = ('host={host} dbname={dbname} '
                        'user={user} password={password}')
    connstr = connstr_template.format(**config.GROUND_DATABASE)
        
    query = '''
        SELECT 
          loc.x, 
          loc.y,
          tsv.scalarvalue
        FROM 
          rgrddata00.timeseriesvaluesandflags tsv, 
          rgrddata00.locations loc, 
          rgrddata00.timeserieskeys tsk, 
          rgrddata00.parameterstable prt
        WHERE  
          prt.id = '{parameter}' AND
          tsv.serieskey = tsk.serieskey AND
          tsk.locationkey = loc.locationkey AND
          tsk.parameterkey = prt.parameterkey AND
          tsv.datetime = '%Y-%m-%d %H:%M:%S';
          '''     
    # Fill query template with unit and datetime
    parameters = {'f': 'WNS1400.5m' , 'h' : 'WNS1400.1h', 'd':'WNS1400.1d'}
    parameter = {'parameter': parameters[timeframe]}
    query = query.format(**parameter)
    query = dt.datetime.strftime(datetime, query)
    
    # Connect to database, query and retrieve rows
    conn = psycopg2.connect(connstr)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    
    # Reshape to list of dicts
    rows = [{'coords': (x, y), 'value': v} for x, y, v in rows]
    return rows
    
if __name__ == '__main__':
    datetime = dt.datetime(2012,1,2,12)
    timeframe = 'f' 
    rows = get_rainstations(datetime, timeframe)  
    
    
