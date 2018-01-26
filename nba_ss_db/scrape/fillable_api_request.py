"""
Contains two classes:
1) FillableAPIRequest: represents multiple requests to make
2) APIRequest: represents a single request to make
"""
import urllib.parse
import itertools
from collections import OrderedDict

from typing import List

from .utils import flatten_list
from .query_param_values import get_possible_query_param_values


VALID_FILLABLES = {'{SEASON}', '{PLAYER_ID}', '{GAME_DATE}', '{DATE_TO}', '{GAME_ID}', '{PLAYER_POSITION}'}

SEASON_DEPENDENT_FILLABLES = ['{PLAYER_ID}', '{GAME_DATE}', '{DATE_TO}', '{GAME_ID}']

OTHER_FILLABLES = VALID_FILLABLES - set(SEASON_DEPENDENT_FILLABLES) - {'{SEASON}'}

class FillableAPIRequest():
    """
    Represents a fillable api request.
    """

    def __init__(self, fillable_api_request: str, is_daily: bool):
        """
        Given a fillable_api_request, parses the fillable choices
        and adds any primary keys if necesssary.
        """
        self.fillable_api_request = fillable_api_request
        self.is_daily = is_daily

        fillable_names, fillable_choices = self._parse_fillable_api_request()
        self.fillable_choices = fillable_names
        self.fillable_names = fillable_choices

    def generate_api_requests(self):
        """
        Yields APIRequest objects that are generated
        by creating every combination of fillable choices.
        """
        for fillable_permutation in itertools.product(*self.fillable_choices):
            fill_mapping = OrderedDict()
            for fillable_type, fillable_value in zip(self.fillable_names, flatten_list(fillable_permutation)):
                fill_mapping[fillable_type] = fillable_value
            yield APIRequest(self.fill_in(**fill_mapping), fill_mapping)

    def fill_in(self, **kwargs):
        return self.fillable_api_request.format(**kwargs)

    def __str__(self):
        return 'Fillable API Request: {}\n\t with fillables: {}'.format(self.fillable_api_request, self.fillable_names)

    def _parse_fillable_api_request(self):
        """
        Parses a fillable api request string by looking for
        specific keywords in the string such as '{SEASON}'
        which denotes that the job should scrape for all
        seasons.
        """
        query_param_names = []
        # a list of lists where a valid api request is formed by
        # picking one from each list (product)
        query_param_choices = []
        if '{SEASON}' in self.fillable_api_request:
            query_param_names.append('SEASON')

            # get which dependent query params are specified
            dependent_query_param_names = []
            for dependent_fillable in SEASON_DEPENDENT_FILLABLES:
                if dependent_fillable in self.fillable_api_request:
                    # remove the curly braces
                    dependent_query_param_names.append(dependent_fillable[1:-1])

            query_param_names.extend(dependent_query_param_names)

            # go through each season and get possible combinations of query params
            grouped_choices = []
            for season in get_possible_query_param_values('{SEASON}', self.is_daily):
                # a list of list of values for each dependent query param
                seasonal_choices = [[season]]

                for dependent_fillable in dependent_query_param_names:
                    dependent_values = get_possible_query_param_values(
                        dependent_fillable, self.is_daily)[season]
                    seasonal_choices.append(dependent_values)

                grouped_choices.extend(itertools.product(*seasonal_choices))

            query_param_choices.append(grouped_choices)
        else:
            # raise an error if there was a dependent query param but no season
            for dependent_query_param in SEASON_DEPENDENT_FILLABLES:
                if dependent_query_param in self.fillable_api_request:
                    raise ValueError(
                        'API request had {} without a {SEASON}.'.format(dependent_query_param))

        for fillable_type in OTHER_FILLABLES:
            if fillable_type in self.fillable_api_request:
                query_param_names.append(fillable_type[1:-1])
                query_param_choices.append(
                    get_possible_query_param_values(fillable_type, self.is_daily))
        
        return query_param_names, query_param_choices


class APIRequest():
    """
    Represents an API request ready to be made.
    A wrapper around a string and the query params
    that make this APIRequest an instance of its job.
    """

    def __init__(self, api_request: str, query_params: dict):
        """
        api_request is a request ready to be made (no formatting needed)
        query_params is a dictionary of params that were filled in
        from the original job.
        """
        self.api_request = api_request
        self.query_params = query_params

    def __str__(self):
        return 'API Request: {}\n with query values: {}'.format(
            self.api_request, self.query_params)
