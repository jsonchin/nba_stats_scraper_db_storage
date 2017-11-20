import argparse
import scrape.scraper
import db.utils
import db.retrieve

parser = argparse.ArgumentParser(description='NBA Stats Scraper and Storage')
parser.add_argument('--scrape', nargs='?', const='api_requests.yaml', dest='scrape_file_path',
                    help="""Scrapes all api requests according to the entries in the supplied file path. If not path is supplied, 'api_requests.yaml' is used.""")
parser.add_argument('--daily_scrape', nargs='?', const='api_requests.yaml', dest='daily_scrape_file_path',
                    help="""Scrapes all api requests according to the entries in the supplied file path for the current season. If not path is supplied, 'api_requests.yaml' is used.""")
parser.add_argument('--training_data', nargs='?', const=None,
                    help="""Queries and stores training data as a csv file. Accepts a FP filter as an optional argument.""")
parser.add_argument('--clear_log', nargs='?', const=None, dest='clear_before_date',
                    help="""Deletes all entries in scrape_log before the supplied date. If no date is supplied all entries are removed.""")
parser.add_argument('--drop_tables', action='store_true',
                    help="""Drops all tables in the database specified in db/config.py.""")


args = parser.parse_args()

if args.drop_tables:
    db.utils.drop_tables()

if args.clear_before_date is not None:
    db.utils.clear_scrape_logs(args.clear_before_date)

if args.scrape_file_path is not None:
    scrape.scraper.run_scrape_jobs(args.scrape_file_path)

if args.daily_scrape_file_path is not None:
    scrape.scraper.run_daily_scrapes(args.daily_scrape_file_path)

if args.training_data is not None:
    db.utils.execute_sql_file('sqlite_cmds.sql')
    db.retrieve.df_to_csv(db.retrieve.aggregate_training_data(filter_fp=int(args.training_data)),
                          'training_data_filter_FP_{}'.format(int(args.training_data)))
