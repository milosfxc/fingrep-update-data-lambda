from datetime import timezone

import config
import db_ops
import fingrep_service
import utils

if __name__ == "__main__":
    import random
    import time
    from datetime import datetime, timedelta

    N = 2_000
    base_time = datetime(2026, 1, 1)

    rows = {}

    for _ in range(N):
        share_id = random.randint(1, 1000)
        dt = base_time + timedelta(seconds=random.randint(0, 86400))

        rows.setdefault(dt, {})[share_id] = {
            "share_id": share_id,
            "datetime": dt
        }
    start = time.perf_counter()

    for dt in sorted(rows):
        for share_id in rows[dt]:
            row = rows[dt][share_id]

    end = time.perf_counter()

    print(f"Nested dict iteration took: {(end - start) * 1000:.3f} ms")