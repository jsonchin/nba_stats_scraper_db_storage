from datetime import datetime
import db.utils


def log_request(api_request):
    curr_time = datetime.now().strftime('%Y-%m-%d %X %f')
    db.utils.execute_sql("""INSERT INTO scrape_log VALUES (?, ?);""", (curr_time, api_request))

def already_scraped(api_request):
    return len(db.utils.execute_sql("""SELECT * FROM scrape_log WHERE api_request = ?;""", (api_request, ))) != 0
