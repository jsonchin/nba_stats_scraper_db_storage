import argparse
import scrape.scraper
import db.utils

parser = argparse.ArgumentParser(description='NBA Stats Scraper and Storage')
parser.add_argument('--scrape', nargs='?', const='api_requests.yaml', dest='scrape_file_path',
                    help="""Scrapes all api requests according to the entries in the supplied file path. If not path is supplied, 'api_requests.yaml' is used.""")
parser.add_argument('--delete', nargs='?', const=None, dest='delete_before_date',
                    help="""Deletes all entries in scrape_log before the supplied date. If no date is supplied all entries are removed.""")

args = parser.parse_args()

if args.scrape_file_path is not None:
    scrape.scraper.run_scrape_jobs(args.scrape_file_path)

if args.delete_before_date is not None:
    db.utils.reset_scrape_logs(args.delete_before_date)
