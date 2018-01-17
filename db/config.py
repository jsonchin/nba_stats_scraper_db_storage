import os
dir = os.path.dirname(__file__)

DB_NAME = 'nba_stats'
DB_PATH = '{}/databases'.format(dir)

CSV_OUTPUT_PATH = 'csv_output'

IGNORE_DUPLICATES = True