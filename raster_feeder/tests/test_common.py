# (c) Nelen & Schuurmans.  GPL licensed, see LICENSE.rst.
# -*- coding: utf-8 -*-

import mock
import os
import unittest
from nose.plugins.attrib import attr

from raster_feeder.common import FTPServer
from raster_feeder.tests.common import TemporaryDirectory


@attr('common')
class TestFTPServer(unittest.TestCase):
    def setUp(self):
        self.login = dict(host='speedtest.tele2.net', user='anonymous',
                          password='')
        self.name = '1KB.zip'

    @mock.patch('raster_feeder.common.ftplib')
    def test_connect(self, ftplib):
        server = FTPServer(**self.login)
        server.close()

        ftplib.FTP.assert_called_with(
            passwd=self.login['password'],
            host=self.login['host'],
            user=self.login['user'],
        )

    @mock.patch('raster_feeder.common.ftplib')
    def test_context_manager(self, ftplib):
        with FTPServer(**self.login) as _:
            pass

    @mock.patch('raster_feeder.common.ftplib')
    def test_get_match(self, ftplib):

        connection = mock.MagicMock()
        connection.nlst.return_value = ['1KB.zip']
        ftplib.FTP.return_value = connection

        with FTPServer(**self.login) as server:
            self.assertIn(self.name, server.listdir())

            match = server.get_latest_match(self.name)
            self.assertEqual(match, self.name)

            match = server.get_latest_match('nomatch')
            self.assertIsNone(match)

    @mock.patch('raster_feeder.common.ftplib')
    def test_download_to_stream(self, ftplib):

        def side_effect_retrbinary(name, callback):
            callback(b' ' * 1024)

        connection = mock.MagicMock()
        connection.retrbinary.side_effect = side_effect_retrbinary
        ftplib.FTP.return_value = connection

        with FTPServer(**self.login) as server:
            stream = server.retrieve_to_stream(self.name)

        self.assertEqual(len(stream.read()), 1024)

    @mock.patch('raster_feeder.common.ftplib')
    def test_download_to_path(self, ftplib):

        def side_effect_retrbinary(name, callback):
            callback(b' ' * 1024)

        connection = mock.MagicMock()
        connection.retrbinary.side_effect = side_effect_retrbinary
        ftplib.FTP.return_value = connection

        with TemporaryDirectory() as path:
            filepath = os.path.join(path, self.name)
            with FTPServer(**self.login) as server:
                server.retrieve_to_path(self.name, filepath)

            with open(filepath, 'rb') as stream:
                self.assertEqual(len(stream.read()), 1024)
