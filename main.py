import datetime

import yahoo_service

df = yahoo_service.get_indices(['^IXIC', '^SPX'], date=datetime.date(2024, 8, 1))
print(df)
