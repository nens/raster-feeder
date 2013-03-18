#!/usr/bin/python
# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import datetime
import logging

from openradar import tasks
from openradar import loghelper
from openradar import config

def main():
    tasks.test_concurrency.delay('a')
    tasks.test_concurrency.delay('b')
