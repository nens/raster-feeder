#!/usr/bin/python
# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from openradar import arguments
from openradar import tasks


def organize(**kwargs):
    task = tasks.organize_from_path
    if kwargs['direct']:
        action = task
    else:
        action = task.delay
    action(source_path=kwargs['source_dir'])


def main():
    argument = arguments.Argument()
    parser = argument.parser(['source_dir', 'direct'])
    organize(**vars(parser.parse_args()))
