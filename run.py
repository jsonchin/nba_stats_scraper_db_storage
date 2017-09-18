import scrape.scraper
import db.utils


if __name__ == '__main__':
    scrape.scraper.run_scrape_jobs('api_requests.yaml')
    db.utils.close_con()
