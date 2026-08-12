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

top_5p_change_conv3 = """
    WITH base AS (
        SELECT
            tf.share_id,
            sh.ticker,
            tf.datetime,
            lag(tf.close, 2) OVER (PARTITION BY tf.share_id ORDER BY tf.datetime) AS close_2,
            lag(tf.close, 3) OVER (PARTITION BY tf.share_id ORDER BY tf.datetime) AS close_3,
            tf.high,
            tf.low,
            lag(tf.high, 1) OVER (PARTITION BY tf.share_id ORDER BY tf.datetime) AS high_1,
            lag(tf.high, 2) OVER (PARTITION BY tf.share_id ORDER BY tf.datetime) AS high_2,
            lag(tf.low, 1) OVER (PARTITION BY tf.share_id ORDER BY tf.datetime) AS low_1,
            lag(tf.low, 2) OVER (PARTITION BY tf.share_id ORDER BY tf.datetime) AS low_2,
            lag(tf.volume, 2) OVER (PARTITION BY tf.share_id ORDER BY tf.datetime) AS volume_2
        FROM timeframe_{timeframe} tf INNER JOIN shares sh ON tf.share_id = sh.id
        WHERE tf.datetime BETWEEN '{dt_start_str}' AND '{dt_end_str}' AND session = 1
    ),
    changes AS (
        SELECT
            share_id,
            ticker,
            datetime,
            high,
            low,
            volume_2,
            (high_2 - low_2) * 0.475 + low_2 as low_1_limit,
            high_1,
            high_2,
            low_1,
            low_2,
            close_2 / close_3 AS change_index
        FROM base
        WHERE close_3 IS NOT NULL
          AND close_2 IS NOT NULL
          AND volume_2 > 100000
    ),
    changes_with_threshold AS (
        SELECT
            *,
            quantile(0.95)(change_index) OVER (PARTITION BY share_id) AS qnt_threshold
        FROM changes
        WHERE isFinite(change_index)
    )
    SELECT
        ticker, datetime
    FROM changes_with_threshold
    WHERE datetime = '{dt_end_str}' AND high_2 >= high_1 AND high_2 >= high AND low_2 < low_1 AND low_1 < low AND low_1_limit <= low_1
      AND change_index >= qnt_threshold
    ORDER BY ticker;
"""
