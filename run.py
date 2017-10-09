import argparse
import scrape.scraper
import db.utils

parser = argparse.ArgumentParser(description='NBA Stats Scraper and Storage')
parser.add_argument('--scrape', nargs='?', const='api_requests.yaml')
parser.add_argument('--delete', nargs='?', const=None,
                    help="""Deletes all entries in scrape_log before the supplied date. If no date is supplied all entries are removed.""")

args = parser.parse_args()

if args.scrape is not None:
    scrape.scraper.run_scrape_jobs(args.scrape)

if args.delete is not None:
    db.utils.reset_scrape_logs(args.delete)