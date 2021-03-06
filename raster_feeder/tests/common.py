# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
# -*- coding: utf-8 -*-

import tempfile
import shutil

from raster_feeder.common import FTPServer

import logging
logging.basicConfig(level=logging.DEBUG)


class TemporaryDirectory(object):
    """
    Context manager for tempfile.mkdtemp().
    This class is available in python +v3.2.
    """
    def __enter__(self):
        self.dir_name = tempfile.mkdtemp()
        return self.dir_name

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self.dir_name)


class MockFTPServer(FTPServer):
    def __init__(self, files):
        self.files = files

    def listdir(self):
        return list(self.files)

    def _retrieve_to_stream(self, name, stream):
        self.files[name].seek(0)
        stream.write(self.files[name].read())
        stream.seek(0)
        return stream
