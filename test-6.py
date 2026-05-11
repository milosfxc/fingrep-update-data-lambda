from datetime import timezone

import config
import db_ops
import fingrep_service
import utils

if __name__ == "__main__":
    loop_days = list(range(3, 5))
    loop_days.reverse()
    for loop_day in loop_days:
        print(utils.get_utc_date(loop_day))
