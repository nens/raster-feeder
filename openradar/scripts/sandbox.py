#!/usr/bin/python
# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import datetime

from openradar import products

def main():
    start = datetime.datetime(2013,3,1,0)
    delta = datetime.timedelta(hours=1)
    end = start + delta
    values = products.get_values_from_opendap(
        x=220,
        y=220,
        start_date=start,
        end_date=end,
    )
    print(values)
