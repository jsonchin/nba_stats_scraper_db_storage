"""
Defines a class representing a json response from stats.nba.com.
"""

from typing import Dict

headers_access = lambda json:json['resultSets'][0]['headers']
row_set_access = lambda json:json['resultSets'][0]['rowSet']

class NBA_response():

    def __init__(self, json_response: Dict):
        # corresponding to how NBA json responses are formatted
        self._headers = headers_access(json_response)
        self._rows = row_set_access(json_response)

    @property
    def headers(self):
        return self._headers

    @property
    def rows(self):
        return self._rows

    def add_season_col(self, season):
        self._headers.append('SEASON')
        for row in self.rows:
            row.append(season)

    def __str__(self):
        return '{} rows with headers: {}'.format(len(self.rows), self.headers)
