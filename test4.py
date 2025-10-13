import config
import polygon_service
from _datetime import datetime, timezone, timedelta

if __name__ == '__main__':
    print(config.date_from)
    print(polygon_service.request_short_volume(['MARA','CVNA']))