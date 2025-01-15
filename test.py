from datetime import datetime, timezone, timedelta

import config
from utils import get_formatted_utc_date
if __name__ == "__main__":
    print(get_formatted_utc_date())
    date_str = datetime.utcnow() - timedelta(days=config.DAYS)
    date_str = date_str.strftime("%Y-%m-%d")
    print(date_str)