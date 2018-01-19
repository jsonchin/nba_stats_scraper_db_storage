import unittest
from tests.test_setup import init_test_db

from nba_ss_db import db
import pandas as pd

from collections import defaultdict

class TestDBStorage(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def setUp(cls):
        init_test_db()


    def test_fetch_player_ids(self):
        """
        Tests that fetch_player_ids returns a dictionary
        with appropriate keys.
        """
        d = db.retrieve.fetch_player_ids()
        self.assertTrue(type(d) == dict or type(d) == defaultdict,
                        'Expected a dictionary got {}'.format(type(d)))

        SEASON_LENGTH = len('2017-18')
        self.assertTrue(all([len(key) == SEASON_LENGTH for key in d.keys()]))


    def test_get_table_names(self):
        """
        Tests that get_table_names excludes certain tables on default.
        """
        table_names = db.retrieve.get_table_names()
        self.assertTrue('scrape_log' not in table_names)
        self.assertTrue('player_ids' not in table_names)


    def test_get_all_table_names(self):
        """
        Tests that get_table_names excludes certain tables on flag False.
        """
        table_names = db.retrieve.get_table_names(only_data=False)
        self.assertTrue('scrape_log' in table_names)
        self.assertTrue('player_ids' in table_names)


    def test_get_column_names(self):
        """
        Tests that get_column names returns a list of strings
        and that the column names are correct and in the correct
        order..
        """
        column_names = db.utils.get_table_column_names('player_ids')
        self.assertTrue(all((type(ele) == str for ele in column_names)))
        self.assertEqual(len(column_names), 3)
        self.assertEqual(column_names[0], 'PLAYER_ID')
        self.assertEqual(column_names[1], 'PLAYER_NAME')
        self.assertEqual(column_names[2], 'SEASON')


    def test_db_utils_execute_sql(self):
        """
        Tests that db.utils.execute_sql returns both column names
        and rows.
        """
        query_result = db.utils.execute_sql("""SELECT * FROM player_ids LIMIT 1;""")
        self.assertEqual(query_result.column_names, ['PLAYER_ID', 'PLAYER_NAME', 'SEASON'])
        self.assertEqual(type(query_result.rows), list)


    def test_db_query(self):
        """
        Tests that db_query returns a Pandas DataFrame
        with appropriate column names.
        """
        df = db.retrieve.db_query("""SELECT * FROM player_ids;""")
        self.assertEqual(type(df), pd.DataFrame)
        self.assertEquals(list(df.columns.values), ['PLAYER_ID', 'PLAYER_NAME', 'SEASON'])
