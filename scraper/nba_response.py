"""
Defines a class representing a json response from stats.nba.com.
"""

from typing import Dict, Any

class NBA_response():

    def __init__(self, json_response: Dict[str: Any]):
        self.headers = []
        self.rows = []