from datetime import datetime
import db.utils


def log_request(api_request):
    curr_time = datetime.now().strftime('%Y-%m-%d %X %f')
    db.utils.execute_sql("""INSERT INTO scrape_log VALUES (?, ?);""", (curr_time, api_request))
