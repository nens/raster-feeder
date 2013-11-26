#!/usr/bin/python
# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from openradar import arguments
from openradar import tasks
from openradar import utils


def nowcast(**kwargs):
    """
    Create nowcast products given some commandline arguments. 
    """
    # Determine action
    task = tasks.nowcast
    if kwargs['direct']:
        action = task
    else:
        action = task.delay
    # Determine derived arguments
    datetimes = utils.MultiDateRange(kwargs['range']).iterdatetimes()
    combinations = utils.get_product_combinations(
        datetimes=datetimes,
        prodcodes=kwargs['prodcodes'],
        timeframes=kwargs['timeframes'])
    # Execute or delay task
    for combination in combinations:
        action_kwargs = dict(result=None,
                             minutes=kwargs['minutes'])
        action_kwargs.update(combination)
        action(**action_kwargs)


def main():
    argument = arguments.Argument()
    parser = argument.parser([
        'range',
        'prodcodes',
        'timeframes',
        'minutes',
        'direct',
    ])
    nowcast(**vars(parser.parse_args()))
