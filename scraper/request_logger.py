from datetime import datetime
from db_storage import db_utils

def log_request(api_request):
    curr_time = datetime.now().strftime('%Y-%m-%d %X %f')
    db_utils.execute_sql("""INSERT INTO scrape_log VALUES (?, ?);""", (curr_time, api_request))
