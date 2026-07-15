from datetime import datetime, timedelta
import time
from config import telegram_logger


def run_scheduler(task, *, every_minutes=None, every_hours=None, second=0):

    while True:
        now = datetime.now()

        if every_minutes:
            next_minute = ((now.minute // every_minutes) + 1) * every_minutes
            if next_minute >= 60:
                next_run = now.replace(minute=0, second=second, microsecond=0)
                next_run += timedelta(hours=1)
            else:
                next_run = now.replace(minute=next_minute, second=second, microsecond=0)
        elif every_hours:
            next_run = now.replace(minute=0, second=second, microsecond=0) + timedelta(hours=every_hours)

        sleep_seconds = (next_run - now).total_seconds()

        time.sleep(sleep_seconds)

        try:
            task()
        except Exception as e:
            telegram_logger.error(f"Scheduled task failed: {e}")