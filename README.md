# NBA Stats Scraper + DB Storage

A tool to scrape stats.nba.com with customizable api requests and to store the json responses into a database for easier queries.

I'm still adding features to this tool (cleaner support for more dynamic api requests, automatic updating of data by daily scraping, etc) but the base features of a data format to automatically scrape for all combinations of query parameters for an api request and database storage are finished.


## Motivation

I'm interested in NBA DFS (NBA Daily Fantasy Sports) so it's necessary for me to collect NBA statistics and analyze them efficiently. Beforehand, I had a data workflow that worked like this:

Let's say we wanted to make this API request:

```
http://stats.nba.com/stats/playergamelog?LeagueID=00&PlayerID=&Season=&SeasonType=Regular+Season
```

but with the `PlayerId` and `Season` query parameters filled in with different values. There's about 400 player ids and let's say 6 seasons that we care about. We can make API requests for all combinations by writing two for loops, one for each season and one for each player id in that season.

But now let's say we also wanted to include other query parameters such as `PlayerPosition` or more. Ideally we would have one process or data format that could handle an arbitrary amount of query parameters and make API requests for all combinations. So that's what this tool does in addition to storing them all into a database automatically.

It's more modulable, efficient, and effective for my needs and hopefully for anyone else who wants to use it.

## Goals

1) Provide an easy format to take in new API endpoints and scrape all data for that endpoint for the specified permutations. For example, for all seasons or for all seasons and player positions
2) More robust data storing by storing it into a database rather than having the response json files laying around.
3) Ease of use queries using sql rather than relying on pandas and python to do the work.
4) Cleaner daily scrapes and daily queries (since NBA DFS is a daily sport).
5) Smoother integration with my NBA DFS dashboard.
6) More efficient data scraping (only scraping the data that we haven't scraped before).


## Overview (example input + output)

This tool takes in the following data format:

```yaml
- DATA_NAME: 'games'
  API_ENDPOINT: 'http://stats.nba.com/stats/leaguegamelog?Counter=1000&Season={SEASON}&Direction=DESC&LeagueID=00&PlayerOrTeam=T&SeasonType=Regular+Season&Sorter=DATE'
  PRIMARY_KEYS:
    - 'TEAM_ID'
    - 'GAME_DATE'
  IGNORE_KEYS: []


- DATA_NAME: 'player_logs'
  API_ENDPOINT: 'http://stats.nba.com/stats/playergamelog?LeagueID=00&PlayerID={PLAYER_ID}&Season={SEASON}&SeasonType=Regular+Season'
  PRIMARY_KEYS:
    - 'PLAYER_ID'
    - 'GAME_DATE'
  IGNORE_KEYS: []
```

Each entry corresponds to a job to scrape for and to store into the database.

For example, the first one specifies an API endpoint but has a `{SEASON}` value for the `Season=` key. So this tool will scrape for all seasons as specified in `config.py`. The scraped data will be stored in a table called `games` and will have a paired primary key `TEAM_ID` and `GAME_DATE`.

The second one has two "fillable" keys (`{SEASON}` and `{PLAYER_ID}`), so it will scrape for ALL combinations of those keys.

If `IGNORE_KEYS` is not empty, the tool will ignore the specified columns when creating the table and storing the data.

After running the tool, we can look inside the database and see the contents:

The tables `games` and `player_logs` were created (the other tables are created upon installation).

```
sqlite> .table
games        player_ids   player_logs  scrape_log 
```

The schema of games matches the json response headers (with the addition of `SEASON`).
```SQL
sqlite> .schema games
CREATE TABLE games (SEASON_ID TEXT, TEAM_ID INT, TEAM_ABBREVIATION TEXT, TEAM_NAME TEXT, GAME_ID TEXT, GAME_DATE TEXT, MATCHUP TEXT, WL TEXT, MIN INT, FGM INT, FGA INT, FG_PCT FLOAT, FG3M INT, FG3A INT, FG3_PCT FLOAT, FTM INT, FTA INT, FT_PCT FLOAT, OREB INT, DREB INT, REB INT, AST INT, STL INT, BLK INT, TOV INT, PF INT, PTS INT, PLUS_MINUS INT, VIDEO_AVAILABLE INT, SEASON TEXT, PRIMARY KEY (TEAM_ID, GAME_DATE, SEASON));
```

The data can be easily queried for analysis.
```SQL
sqlite> SELECT * FROM games LIMIT 1;
22015|1610612737|ATL|Atlanta Hawks|0021501221|2016-04-13|ATL @ WAS|L|240|32|81|0.395|11|30|0.367|23|31|0.742|9|38|47|22|13|5|23|21|98|-11|1|2015-16
```

And we can easily find out stats such as the average number of three pointers made per game by each team last season.
```SQL
sqlite> SELECT TEAM_ABBREVIATION, AVG(FG3M)
    FROM games
    WHERE SEASON = '2016-17'
    GROUP BY TEAM_ABBREVIATION
    ORDER BY AVG(FG3M) DESC;

HOU|14.4024390243902
CLE|13.0121951219512
BOS|12.0121951219512
GSW|11.9756097560976
DAL|10.7073170731707
BKN|10.6951219512195
DEN|10.609756097561
POR|10.390243902439
LAC|10.2560975609756
PHI|10.1341463414634
CHA|10.0487804878049
MIA|9.85365853658537
UTA|9.64634146341463
NOP|9.36585365853658
MEM|9.35365853658537
WAS|9.21951219512195
SAS|9.18292682926829
SAC|8.98780487804878
LAL|8.90243902439024
ATL|8.89024390243902
TOR|8.84146341463415
MIL|8.78048780487805
IND|8.64634146341463
NYK|8.58536585365854
ORL|8.54878048780488
OKC|8.4390243902439
DET|7.69512195121951
CHI|7.59756097560976
PHX|7.5
MIN|7.32926829268293
```

Looks like Boston did pretty well in terms of three pointers! Gotta give it to IT.



## Instructions

To install and use, set up your virtualenv at the root of the repository.

```
cd nba_stats_scraper_db_storage
pip3 install virtualenv
virtualenv venv
. venv/bin/activate
```

Then install the packages.

```
python3 setup.py install
```

and run the tool.

```
python3 run.py --scrape
```

which will run scrape on `api_requests.yaml` if no other argument (path to another yaml file) to `--scrape` is passed in.

A few examples have already been provided in `api_requests.yaml` but if you want to add more, just follow the provided format.

api_requests.yaml:
```yaml
- DATA_NAME: 'games'
  API_ENDPOINT: 'http://stats.nba.com/stats/leaguegamelog?Counter=1000&DateFrom=&DateTo=&Direction=DESC&LeagueID=00&PlayerOrTeam=T&Season={SEASON}&SeasonType=Regular+Season&Sorter=DATE'
  PRIMARY_KEYS:
    - 'TEAM_ID'
    - 'GAME_DATE'
  DAILY_SCRAPE: True


- DATA_NAME: 'player_logs'
  API_ENDPOINT: 'http://stats.nba.com/stats/playergamelog?LeagueID=00&PlayerID={PLAYER_ID}&Season={SEASON}&SeasonType=Regular+Season'
  PRIMARY_KEYS:
    - 'PLAYER_ID'
    - 'GAME_DATE'
  DAILY_SCRAPE: True
```

##### Utilities:

```
python3 run.py --clear_log {YYYY-MM-DD}
```

will clear all entries from scrape_log before the supplied date. If not date is supplied, all entries will be deleted.
