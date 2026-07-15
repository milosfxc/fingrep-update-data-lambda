conv3_gap10 = """
WITH tfd_x1 AS (
    SELECT
        tfd.share_id AS id,
        tfd.high AS value0,
        tfd.low AS value1
    FROM {table} tfd
    WHERE tfd.{temporal} = %(temporal_1)s
),
tfd_x2 AS (
    SELECT
        tfd.share_id AS id,
        tfd.high AS value0,
        tfd.low AS value1
    FROM {table} tfd
    WHERE tfd.{temporal} = %(temporal_2)s
)
SELECT
    s.ticker,
    s.id
FROM shares s
JOIN d_timeframe tfd
    ON tfd.share_id = s.id
   AND tfd.date = %(temporal_0)s
JOIN tfd_x1
    ON tfd_x1.id = s.id
JOIN tfd_x2
    ON tfd_x2.id = s.id
WHERE tfd.rel_gap > 100000
  AND tfd_x2.value0 >= tfd_x1.value0
  AND tfd_x2.value0 >= tfd.high
  AND tfd_x2.value1 < tfd_x1.value1
  AND tfd_x2.value1 < tfd.low;
"""


conv3 = """
WITH tfd_x1 AS (
    SELECT
        tfd.share_id AS id,
        tfd.high AS value0,
        tfd.low AS value1
    FROM {table} tfd
    WHERE tfd.{temporal} = %(temporal_1)s
),
tfd_x2 AS (
    SELECT
        tfd.share_id AS id,
        tfd.high AS value0,
        tfd.low AS value1
    FROM {table} tfd
    WHERE tfd.{temporal} = %(temporal_2)s
)
SELECT
    s.ticker,
    s.id
FROM shares s
JOIN d_timeframe tfd
    ON tfd.share_id = s.id
   AND tfd.date = %(temporal_0)s
JOIN tfd_x1
    ON tfd_x1.id = s.id
JOIN tfd_x2
    ON tfd_x2.id = s.id
WHERE tfd_x2.value0 >= tfd_x1.value0
  AND tfd_x2.value0 >= tfd.high
  AND tfd_x2.value1 < tfd_x1.value1
  AND tfd_x2.value1 < tfd.low;
"""

