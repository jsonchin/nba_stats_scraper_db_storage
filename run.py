import argparse
import scrape.scraper

parser = argparse.ArgumentParser(description='NBA Stats Scraper and Storage')
parser.add_argument('--scrape', nargs='?', const='api_requests.yaml')

args = parser.parse_args()

if args.scrape is not None:
    scrape.scraper.run_scrape_jobs(args.scrape)
