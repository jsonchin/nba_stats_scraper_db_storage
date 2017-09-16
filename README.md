# NBA Stats Scraper + DB Storage

A tool to scrape stats.nba.com with customizable api requests and to store the json responses into a database for easier queries.

This tool is meant for personal use but anyone interested in nba statistics or data science should feel free to use it! It's still a work in progress (I'm currently rewriting some of my old code that I used a for my old data pipeline) and should be done before October when the NBA season starts.

## Motivation

I'm interested in NBA DFS (NBA Daily Fantasy Sports) so it's necessary for me to collect NBA statistics and analyze them. Beforehand, I had a data workflow that worked like this:

1) I was curious about a new statistic and scraped it from stats.nba.com writing a new Python function to make the request to that API endpoint.
2) Retrieve the old data stored as json and aggregate them using pandas (having to write more Python code).

While that was alright, I felt I needed a cleaner, more automatic workflow and more modularability.

## Goals

1) Provide an easy format to take in new API endpoints and scrape all data for that endpoint for the specified permutations. For example, for all seasons or for all seasons and player positions
2) More robust data storing by storing it into a database rather than plain json files laying around.
3) Ease of use queries rather than relying on pandas and python to do the work.
4) Cleaner daily scrapes and daily queries (since NBA DFS is a daily sport).
5) Smoother integration with my NBA DFS dashboard (before my dashboard would just read from json).


### Spec

Fill in a data format in yaml or json, specifying which api endpoints to scrape and how to format them in a database.

```yaml
- DATA_NAME: 'player_ids'
  API_ENDPOINT: 'http://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight='
  PRIMARY_KEYS:
    - 'PLAYER_ID'
    - 'PLAYER_NAME'
  DAILY_SCRAPE: False
```

This tool should be able to read in api endpoint string, see `{season}` scrape data for all seasons specified in the config file, create a table if needed with the specified `DATA_NAME`, with types specified in the json response, and with primary keys under `PRIMARY_KEYS`. This should automate a lot of work for me.

