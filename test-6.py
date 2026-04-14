from datetime import timezone

import config
import utils

if __name__ == "__main__":
    import datetime
    dt = datetime.datetime.fromtimestamp(1610144640000/1000,timezone.utc).replace(tzinfo=None)
    print(dt)
    print(dt.hour * 100 + dt.minute)