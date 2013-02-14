#!/usr/bin/python
# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import datetime

from openradar import products
from pylab import *

def main():
    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(days=1)
    print('getting values')
    values = products.get_values_from_opendap(x=200, y=200, start_date=start, end_date=end)
    print('got values')
    plot([v['datetime'] for v in values],[v['value'] for v in values])
    show()

