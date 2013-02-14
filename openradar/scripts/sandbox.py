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
    end = datetime.datetime.utcnow().replace(month=2)
    start = end - datetime.timedelta(days=14)
    import pprint
    pprint.pprint(products.get_values_from_opendap(x=200, y=200, start_date=start, end_date=end))
