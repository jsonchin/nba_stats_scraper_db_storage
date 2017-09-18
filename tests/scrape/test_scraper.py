import unittest

from db import config

config.DB_NAME = 'test_db'
config.DB_PATH = 'tests/db/databases'

import db.request_logger


class TestScraper(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def setUp(cls):
        pass

    def test_request_logger(self):
        db.request_logger.log_request('api_request')
