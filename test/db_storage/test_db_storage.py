import unittest
from db_storage import db_config
db_config.DB_NAME = 'test_db'
db_config.DB_PATH = 'test/db_storage/databases'

from db_storage import db_init
from db_storage import db_storage


class TestDBStorage(unittest.BaseTestSuite):

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def setUp(cls):
        pass

    def test_db_init(self):
        db_init.init_db()

    def test_exists_db(self):
        db_storage.exists_table('TABLE_NAME_THAT_DOES_NOT_EXIST')
