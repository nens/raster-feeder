#!/usr/bin/python
# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import logging
import sys

from openradar import arguments
from openradar import files


def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    argument = arguments.Argument()
    parser = argument.parser(['source_dir'])
    files.organize_from_path(**vars(parser.parse_args()))
